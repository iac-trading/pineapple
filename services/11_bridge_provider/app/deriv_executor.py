import os
import json
import asyncio
import websockets
import logging
import redis as redis_lib
from datetime import datetime, timezone

logger = logging.getLogger("DerivExecutor")

DERIV_WS_URL = os.getenv("DERIV_WS_URL", "wss://ws.derivws.com/websockets/v3")
REDIS_URL    = os.getenv("REDIS_URL", "redis://192.168.100.200:6379/0")

# ──────────────────────────────────────────────────────────────────────────────
# ACCOUNT ALIASES
# Los tokens reales están en variables de entorno (Ansible Vault).
# Redis solo almacena el ALIAS de la cuenta (p.ej. "cuenta_a"), nunca el token.
#
# Configura múltiples cuentas en Ansible:
#   DERIV_TOKEN_CUENTA_A=<token>   DERIV_APP_ID_CUENTA_A=<app_id>
#   DERIV_TOKEN_CUENTA_B=<token>   DERIV_APP_ID_CUENTA_B=<app_id>
#   DERIV_TOKEN_DEFAULT=<token>    DERIV_APP_ID_DEFAULT=<app_id>
# ──────────────────────────────────────────────────────────────────────────────

def _build_account_map() -> dict:
    """
    Construye un dict {alias: {token, app_id}} leyendo todas las vars de entorno
    con el patrón DERIV_TOKEN_<ALIAS> / DERIV_APP_ID_<ALIAS>.
    Siempre incluye 'default' del par DERIV_TOKEN / DERIV_APP_ID legados.
    """
    accounts = {}
    # Alias explícitos: DERIV_TOKEN_CUENTA_A, DERIV_TOKEN_CUENTA_B …
    for key, val in os.environ.items():
        if key.startswith("DERIV_TOKEN_") and not key.endswith("_DEFAULT"):
            alias = key[len("DERIV_TOKEN_"):].lower()
            app_id_key = f"DERIV_APP_ID_{alias.upper()}"
            accounts[alias] = {
                "token":  val,
                "app_id": os.getenv(app_id_key, os.getenv("DERIV_APP_ID", ""))
            }
    # Default (legacy: DERIV_TOKEN + DERIV_APP_ID)
    default_token  = os.getenv("DERIV_TOKEN", "")
    default_app_id = os.getenv("DERIV_APP_ID", "")
    if default_token:
        accounts["default"] = {"token": default_token, "app_id": default_app_id}
    return accounts


class DerivExecutor:
    def __init__(self, app_id: str, token: str):
        # Legacy: usado como fallback si Redis no devuelve alias
        self._default_app_id = app_id
        self._default_token  = token

        # Mapa completo de cuentas (token real nunca va a Redis)
        self._accounts: dict = _build_account_map()
        if "default" not in self._accounts:
            self._accounts["default"] = {"token": token, "app_id": app_id}

        self._last_order_ts: float = 0
        # Para modo multiplier: {symbol: contract_id}
        self._open_contracts: dict[str, int] = {}

        # Redis para config dinámica (modo, cuenta activa, SL/TP)
        try:
            self._redis = redis_lib.from_url(REDIS_URL)
            self._redis.ping()
            logger.info(f"✅ DerivExecutor: Redis conectado → {REDIS_URL}")
        except Exception as e:
            self._redis = None
            logger.warning(f"⚠️ DerivExecutor: Redis no disponible → {e}. Usando defaults de env.")

        logger.info(f"DerivExecutor: {len(self._accounts)} cuenta(s) configurada(s): {list(self._accounts)}")

    # ── HELPERS DE CONFIG DINÁMICA ────────────────────────────────────────────
    def _redis_get(self, key: str, default: str = "") -> str:
        if not self._redis:
            return default
        try:
            val = self._redis.get(key)
            return val.decode() if val else default
        except Exception:
            return default

    def _get_account(self, symbol: str) -> dict:
        """Resuelve el alias de cuenta para el símbolo, via Redis → env vars."""
        alias = self._redis_get(f"bridge:active_account:{symbol}", "default")
        account = self._accounts.get(alias) or self._accounts.get("default", {})
        logger.debug(f"Account for {symbol}: alias='{alias}' → app_id={account.get('app_id', '?')[:8]}…")
        return account

    def _get_contract_mode(self, symbol: str) -> str:
        env_default = os.getenv("DERIV_CONTRACT_MODE", "binary")
        return self._redis_get(f"bridge:contract_mode:{symbol}", env_default)

    def _get_multiplier(self, symbol: str) -> int:
        env_default = os.getenv("DERIV_MULTIPLIER", "10")
        return int(self._redis_get(f"bridge:multiplier:{symbol}", env_default))

    def _get_sl(self, symbol: str) -> float:
        env_default = os.getenv("DERIV_STOP_LOSS_USD", "1.0")
        return float(self._redis_get(f"bridge:sl_usd:{symbol}", env_default))

    def _get_tp(self, symbol: str) -> float:
        env_default = os.getenv("DERIV_TAKE_PROFIT_USD", "2.0")
        return float(self._redis_get(f"bridge:tp_usd:{symbol}", env_default))

    async def _rate_limit_wait(self):
        now     = asyncio.get_event_loop().time()
        elapsed = now - self._last_order_ts
        if elapsed < 0.3:
            await asyncio.sleep(0.3 - elapsed)
        self._last_order_ts = asyncio.get_event_loop().time()

    # ── ROUTER PRINCIPAL ─────────────────────────────────────────────────────
    async def place_market_order(self, symbol: str, side: str, qty: float, meta: dict = None):
        """
        Consulta Redis para determinar dinámicamente:
          - ¿Qué cuenta usar? (alias → token real en env)
          - ¿Modo binary o multiplier?
          - TP / SL en USD (solo multiplier)
        """
        meta = meta or {}
        mode = self._get_contract_mode(symbol)
        account = self._get_account(symbol)

        logger.info(
            f"[ORDER] symbol={symbol} side={side} qty={qty} "
            f"mode={mode} account={account.get('app_id','?')[:8]}…"
        )

        if mode == "multiplier":
            sl  = meta.get("stop_loss",  self._get_sl(symbol))
            tp  = meta.get("take_profit", self._get_tp(symbol))
            mul = self._get_multiplier(symbol)
            return await self._place_multiplier_order(symbol, side, qty, account, mul, sl, tp)
        else:
            return await self._place_binary_order(symbol, side, qty, account)

    # ── MODO 1: BINARY OPTIONS ───────────────────────────────────────────────
    async def _place_binary_order(self, symbol: str, side: str, qty: float, account: dict):
        await self._rate_limit_wait()
        url = f"{DERIV_WS_URL}?app_id={account['app_id']}"

        async with websockets.connect(url) as ws:
            await ws.send(json.dumps({"authorize": account["token"]}))
            auth_resp = json.loads(await ws.recv())
            if "error" in auth_resp:
                raise RuntimeError(f"Deriv Auth Error: {auth_resp['error']}")

            auth_data = auth_resp.get("authorize", {})
            logger.info(f"[BINARY] Auth OK: ID={auth_data.get('loginid')} Bal={auth_data.get('balance')} USD")

            contract_type = "CALL" if side.lower() in ("buy", "long") else "PUT"
            payload = {
                "buy": 1, "price": qty,
                "parameters": {
                    "amount": qty, "contract_type": contract_type,
                    "symbol": symbol, "basis": "stake", "currency": "USD",
                    "duration": 5, "duration_unit": "m"
                }
            }
            await ws.send(json.dumps(payload))
            resp = json.loads(await ws.recv())

            if "error" in resp:
                raise RuntimeError(f"Deriv Execution Error: {resp['error']}")

            buy_data    = resp.get("buy", {})
            contract_id = buy_data.get("contract_id")
            entry_px    = float(buy_data.get("entry_tick") or buy_data.get("entry_spot") or 0.0)
            if entry_px == 0.0 and contract_id:
                entry_px = await self._recover_entry_price(contract_id, account)

            logger.info(f"[BINARY] OK: contract={contract_id} spot={entry_px} stake={buy_data.get('buy_price')}")
            return {"broker_order_id": str(contract_id), "status": "filled",
                    "price": entry_px, "broker": "deriv", "contract_mode": "binary", "raw": resp}

    # ── MODO 2: MULTIPLIERS ──────────────────────────────────────────────────
    async def _place_multiplier_order(
        self, symbol: str, side: str, qty: float,
        account: dict, multiplier: int, sl_usd: float, tp_usd: float
    ):
        await self._rate_limit_wait()
        url = f"{DERIV_WS_URL}?app_id={account['app_id']}"
        contract_type = "MULTUP" if side.lower() in ("buy", "long") else "MULTDOWN"

        async with websockets.connect(url) as ws:
            await ws.send(json.dumps({"authorize": account["token"]}))
            auth_resp = json.loads(await ws.recv())
            if "error" in auth_resp:
                raise RuntimeError(f"Deriv Auth Error: {auth_resp['error']}")

            auth_data = auth_resp.get("authorize", {})
            logger.info(f"[MULT] Auth OK: ID={auth_data.get('loginid')} Bal={auth_data.get('balance')} USD")

            # Cerrar posición existente si la hay (para este símbolo)
            if symbol in self._open_contracts:
                existing = self._open_contracts.pop(symbol)
                logger.info(f"[MULT] Closing existing contract {existing} before opening {side}")
                await ws.send(json.dumps({"sell": existing, "price": 0}))
                sell_resp = json.loads(await ws.recv())
                if "error" in sell_resp:
                    logger.warning(f"[MULT] Could not close prior contract: {sell_resp['error']}")
                else:
                    logger.info(f"[MULT] Closed at {sell_resp.get('sell', {}).get('sold_for', '?')} USD")

            payload = {
                "buy": 1, "price": qty,
                "parameters": {
                    "contract_type": contract_type, "amount": qty,
                    "basis": "stake", "symbol": symbol, "currency": "USD",
                    "multiplier": multiplier,
                    "stop_loss": sl_usd, "take_profit": tp_usd
                }
            }
            logger.info(f"[MULT] Opening {contract_type} | {symbol} stake=${qty} ×{multiplier} SL=${sl_usd} TP=${tp_usd}")
            await ws.send(json.dumps(payload))
            resp = json.loads(await ws.recv())

            if "error" in resp:
                raise RuntimeError(f"Deriv Multiplier Error: {resp['error']}")

            buy_data    = resp.get("buy", {})
            contract_id = buy_data.get("contract_id")
            entry_px    = float(buy_data.get("entry_tick") or buy_data.get("entry_spot") or 0.0)
            if entry_px == 0.0 and contract_id:
                entry_px = await self._recover_entry_price(contract_id, account)
            if contract_id:
                self._open_contracts[symbol] = contract_id

            logger.info(f"[MULT] OK: contract={contract_id} spot={entry_px} SL=${sl_usd} TP=${tp_usd}")
            return {
                "broker_order_id": str(contract_id), "status": "filled",
                "price": entry_px, "broker": "deriv", "contract_mode": "multiplier",
                "multiplier": multiplier, "stop_loss_usd": sl_usd, "take_profit_usd": tp_usd, "raw": resp
            }

    async def close_multiplier_position(self, symbol: str, account: dict = None) -> dict:
        """Cierra manualmente un contrato multiplier abierto."""
        contract_id = self._open_contracts.pop(symbol, None)
        if not contract_id:
            return {"status": "no_position"}
        account = account or self._get_account(symbol)
        url = f"{DERIV_WS_URL}?app_id={account['app_id']}"
        async with websockets.connect(url) as ws:
            await ws.send(json.dumps({"authorize": account["token"]}))
            auth_resp = json.loads(await ws.recv())
            if "error" in auth_resp:
                raise RuntimeError(f"Deriv Auth Error (close): {auth_resp['error']}")
            await ws.send(json.dumps({"sell": contract_id, "price": 0}))
            resp = json.loads(await ws.recv())
            if "error" in resp:
                return {"status": "error", "error": resp["error"]}
            sold_for = resp.get("sell", {}).get("sold_for", 0.0)
            logger.info(f"[MULT] Closed contract={contract_id} for ${sold_for}")
            return {"broker_order_id": str(contract_id), "status": "closed",
                    "price": float(sold_for), "broker": "deriv", "raw": resp}

    # ── PRECIO DE ENTRADA ────────────────────────────────────────────────────
    async def _recover_entry_price(self, contract_id: int, account: dict) -> float:
        url = f"{DERIV_WS_URL}?app_id={account['app_id']}"
        try:
            async with websockets.connect(url, open_timeout=5) as ws:
                await ws.send(json.dumps({"authorize": account["token"]}))
                await ws.recv()
                await ws.send(json.dumps({"proposal_open_contract": 1, "contract_id": contract_id}))
                for _ in range(8):
                    resp = json.loads(await ws.recv())
                    if resp.get("msg_type") == "proposal_open_contract":
                        poc   = resp.get("proposal_open_contract", {})
                        price = float(poc.get("entry_tick") or poc.get("entry_spot") or 0.0)
                        if price > 0:
                            return price
        except Exception as e:
            logger.warning(f"Price recovery failed: {e}")
        return 0.0

    # ── PORTFOLIO / BALANCE ──────────────────────────────────────────────────
    async def get_positions(self):
        account = self._get_account("default")
        url = f"{DERIV_WS_URL}?app_id={account['app_id']}"
        async with websockets.connect(url) as ws:
            await ws.send(json.dumps({"authorize": account["token"]}))
            auth_resp = json.loads(await ws.recv())
            if "error" in auth_resp:
                return []
            await ws.send(json.dumps({"portfolio": 1}))
            resp = json.loads(await ws.recv())
            if "error" in resp:
                return []
            return [
                {"symbol": c.get("symbol"), "qty": float(c.get("buy_price", 0.0)),
                 "avg_price": float(c.get("entry_tick", 0.0)), "broker": "deriv", "raw": c}
                for c in resp.get("portfolio", {}).get("contracts", [])
            ]

    async def get_balance(self):
        account = self._get_account("default")
        url = f"{DERIV_WS_URL}?app_id={account['app_id']}"
        async with websockets.connect(url) as ws:
            await ws.send(json.dumps({"authorize": account["token"]}))
            auth_resp = json.loads(await ws.recv())
            if "error" in auth_resp:
                return {"total": 0.0, "currency": "USD"}
            auth_data = auth_resp.get("authorize", {})
            return {"total": float(auth_data.get("balance") or 0.0),
                    "currency": auth_data.get("currency", "USD")}
