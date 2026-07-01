import ccxt.async_support as ccxt
import logging
import os

logger = logging.getLogger("CcxtClient")

class CcxtClient:
    def __init__(self, exchange_id: str = "binance"):
        self.exchange_id = exchange_id
        self.api_key = os.getenv(f"{exchange_id.upper()}_API_KEY")
        self.secret = os.getenv(f"{exchange_id.upper()}_SECRET")
        self.exchange = None

    async def _init_exchange(self):
        if self.exchange:
            return
        
        # Detect paper/sandbox mode
        is_paper = "_paper" in self.exchange_id.lower()
        real_exch_id = self.exchange_id.replace("_paper", "")
        
        # Load keys (try _PAPER_ suffix first if in paper mode)
        key_env = f"{real_exch_id.upper()}_PAPER_API_KEY" if is_paper else f"{real_exch_id.upper()}_API_KEY"
        secret_env = f"{real_exch_id.upper()}_PAPER_SECRET" if is_paper else f"{real_exch_id.upper()}_SECRET"
        
        self.api_key = os.getenv(key_env) or os.getenv(f"{real_exch_id.upper()}_API_KEY")
        self.secret = os.getenv(secret_env) or os.getenv(f"{real_exch_id.upper()}_SECRET")

        if not self.api_key or not self.secret:
            logger.warning(f"API Key or Secret missing for {self.exchange_id}. Simulation mode only.")
            
        exchange_class = getattr(ccxt, real_exch_id)
        self.exchange = exchange_class({
            'apiKey': self.api_key,
            'secret': self.secret,
            'enableRateLimit': True,
            'options': {'defaultType': 'spot'}
        })
        
        if is_paper:
            try:
                self.exchange.set_sandbox_mode(True)
                logger.info(f"CcxtClient ({self.exchange_id}): SANDBOX MODE ENABLED.")
            except Exception as e:
                logger.warning(f"CcxtClient ({self.exchange_id}): Could not enable sandbox mode: {e}")

        logger.info(f"CcxtClient ({self.exchange_id}): Rate limiter initialized.")
        # Additional safety delay for live trading
        self._last_order_ts = 0
        self._order_lock = asyncio.Lock()

    async def _rate_limit_wait(self):
        async with self._order_lock:
            now = asyncio.get_event_loop().time()
            # Ensure at least 500ms between orders as an extra safety layer
            elapsed = now - self._last_order_ts
            if elapsed < 0.5:
                await asyncio.sleep(0.5 - elapsed)
            self._last_order_ts = asyncio.get_event_loop().time()

    async def place_order(self, symbol: str, side: str, qty: float, order_type: str = "market"):
        await self._init_exchange()
        await self._rate_limit_wait()
        
        logger.info(f"CCXT: Placing {order_type} {side} order for {symbol} qty={qty}")
        
        if not self.api_key:
            # Simulation for dev if keys are missing
            return {
                "status": "simulated_filled",
                "price": 0.0,
                "broker_order_id": f"ccxt-sim-{self.exchange_id}",
                "broker": self.exchange_id
            }

        try:
            params = {}
            # For market orders, some exchanges require 'params' or 'amount' mapping
            if side.lower() == "buy":
                order = await self.exchange.create_order(symbol, order_type, 'buy', qty, params=params)
            else:
                order = await self.exchange.create_order(symbol, order_type, 'sell', qty, params=params)
                
            status = str(order['status']).lower()
            if status == "closed":
                status = "filled"
            elif status == "open":
                status = "partially_filled" if float(order.get('filled', 0.0)) > 0 else "open"

            return {
                "status": status,
                "price": float(order.get('price') or order.get('average') or 0.0),
                "broker_order_id": str(order['id']),
                "broker": self.exchange_id,
                "raw": order
            }
        except Exception as e:
            logger.error(f"CCXT Error ({self.exchange_id}): {e}")
            raise

    async def close(self):
        if self.exchange:
            await self.exchange.close()

    async def get_positions(self):
        await self._init_exchange()
        if not self.api_key:
            return []
        
        try:
            res = []
            if self.exchange.has['fetchPositions']:
                positions = await self.exchange.fetch_positions()
                for pos in positions:
                    if float(pos.get('contracts', 0) or pos.get('amount', 0)) != 0:
                        res.append({
                            "symbol": pos['symbol'],
                            "qty": float(pos['contracts'] or pos['amount']),
                            "avg_price": float(pos.get('entryPrice') or 0.0),
                            "broker": self.exchange_id,
                            "raw": pos
                        })
            else:
                balance = await self.exchange.fetch_balance()
                for asset, data in balance['total'].items():
                    if data != 0:
                        res.append({
                            "symbol": asset,
                            "qty": float(data),
                            "avg_price": 0.0,
                            "broker": self.exchange_id,
                            "raw": balance[asset] if asset in balance else {}
                        })
            return res
        except Exception as e:
            logger.error(f"CCXT Error fetching positions ({self.exchange_id}): {e}")
            return []
