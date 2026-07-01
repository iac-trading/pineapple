import sys
import os
import json
import asyncio
import logging
import httpx
import functools
from datetime import datetime, timezone
from nats.aio.client import Client as NATS

print = functools.partial(print, flush=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] AlertManager: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("AlertManager")

NATS_URL          = os.getenv("NATS_URL", "nats://192.168.100.200:4222")
TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN", "")

# Múltiples destinatarios: TELEGRAM_CHAT_IDS = "id1,id2,id3"
# También soporta la variable legacy TELEGRAM_CHAT_ID para compatibilidad.
_raw_ids = os.getenv("TELEGRAM_CHAT_IDS") or os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_CHAT_IDS = [cid.strip() for cid in _raw_ids.split(",") if cid.strip()]

# Umbrales configurables
BALANCE_WARNING_USD = float(os.getenv("BALANCE_WARNING_USD", "5.0"))   # Alerta si balance < $5
BALANCE_CRITICAL_USD = float(os.getenv("BALANCE_CRITICAL_USD", "2.0")) # Alerta crítica si < $2

# Emojis por nivel
LEVEL_EMOJI = {"info": "ℹ️", "warning": "⚠️", "critical": "🚨", "trade": "💹"}

# ──────────────────────────────────────────────────────────────
class AlertManager:
    def __init__(self):
        self.nc = NATS()
        self._last_balance = None
        self._balance_warned = False

    # ── CORE: enviar a TODOS los destinatarios ──────────────────
    async def broadcast(self, text: str, level: str = "info"):
        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_IDS:
            logger.warning("⚠️ Telegram no configurado. Alerta omitida.")
            return
        emoji = LEVEL_EMOJI.get(level, "ℹ️")
        full_text = f"{emoji} *AXIO\\-QUANT ALERT*\n{text}"
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        async with httpx.AsyncClient(timeout=10) as client:
            for chat_id in TELEGRAM_CHAT_IDS:
                try:
                    resp = await client.post(url, json={
                        "chat_id": chat_id,
                        "text": full_text,
                        "parse_mode": "MarkdownV2"
                    })
                    resp.raise_for_status()
                    logger.info(f"✅ Telegram enviado → chat_id={chat_id}")
                except Exception as e:
                    logger.error(f"❌ Telegram FAILED → chat_id={chat_id}: {e}")

    # ── SUBSCRIPTIONS ───────────────────────────────────────────
    async def start(self):
        await self.nc.connect(
            servers=[NATS_URL],
            max_reconnect_attempts=30,
            reconnect_time_wait=3,
            reconnected_cb=self.on_reconnected,
            disconnected_cb=self.on_disconnected,
        )
        logger.info(f"📱 AlertManager Online → {len(TELEGRAM_CHAT_IDS)} destinatarios Telegram")

        await self.nc.subscribe("alerts.critical",       cb=self.on_critical_alert)
        await self.nc.subscribe("alerts.reconciliation", cb=self.on_reconciliation_alert)
        await self.nc.subscribe("alerts.balance",        cb=self.on_balance_alert)
        await self.nc.subscribe("alerts.signal",         cb=self.on_signal_alert)
        await self.nc.subscribe("orders.events",         cb=self.on_order_event)
        await self.nc.subscribe("bridge.balance.reply",  cb=self.on_balance_reply)

        # Enviar notificación de arranque del sistema
        await self.broadcast(
            f"🚀 *Sistema arrancado*\n"
            f"🕒 `{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC`\n"
            f"📡 AlertManager conectado\n"
            f"👥 {len(TELEGRAM_CHAT_IDS)} destinatarios registrados",
            level="info"
        )

        # Polling de balance cada 5 minutos
        asyncio.create_task(self.balance_monitor_loop())

        while True:
            await asyncio.sleep(60)

    # ── BALANCE MONITOR ────────────────────────────────────────
    async def balance_monitor_loop(self):
        """Solicita el balance cada 5 minutos y alerta si está bajo."""
        await asyncio.sleep(30)  # Arrancar 30s después del boot
        while True:
            try:
                resp = await self.nc.request("bridge.balance.get", b"", timeout=5)
                data = json.loads(resp.data.decode())
                if data.get("status") == "ok":
                    balance = float(data.get("total_equity", 0))
                    self._last_balance = balance
                    logger.info(f"💰 Balance actual: ${balance:.2f}")

                    if balance <= BALANCE_CRITICAL_USD:
                        await self.broadcast(
                            f"🔴 *CAPITAL CRÍTICO*\n"
                            f"💰 Balance: `${balance:.2f}`\n"
                            f"⚠️ Por debajo del límite crítico de `${BALANCE_CRITICAL_USD:.2f}`\n"
                            f"🛑 Considera detener estrategias manualmente\\.",
                            level="critical"
                        )
                    elif balance <= BALANCE_WARNING_USD and not self._balance_warned:
                        await self.broadcast(
                            f"🟡 *ALERTA DE CAPITAL BAJO*\n"
                            f"💰 Balance: `${balance:.2f}`\n"
                            f"📉 Por debajo del umbral de advertencia de `${BALANCE_WARNING_USD:.2f}`",
                            level="warning"
                        )
                        self._balance_warned = True
                    elif balance > BALANCE_WARNING_USD:
                        self._balance_warned = False  # Reset si se recupera
            except Exception as e:
                logger.warning(f"Balance monitor error: {e}")
            await asyncio.sleep(300)  # Cada 5 minutos

    # ── HANDLERS ───────────────────────────────────────────────
    async def on_reconnected(self):
        logger.info("🌐 NATS RECONECTADO")
        await self.broadcast(
            "🔄 *Reconexión detectada*\n"
            "El sistema se reconectó a NATS exitosamente\\.",
            level="warning"
        )

    async def on_disconnected(self):
        logger.warning("🌐 NATS DESCONECTADO. Reintentando...")

    async def on_critical_alert(self, msg):
        try:
            data = json.loads(msg.data.decode())
            text = (
                f"💬 *Mensaje*: {self._esc(data.get('msg', 'Sin descripción'))}\n"
                f"🕒 `{data.get('ts', 'N/A')}`\n"
                f"📂 Fuente: `{self._esc(data.get('source', 'desconocida'))}`"
            )
            if "details" in data:
                text += f"\n🔍 *Detalles:*\n`{self._esc(str(data['details']))}`"
            await self.broadcast(text, level="critical")
        except Exception as e:
            logger.error(f"Error on_critical_alert: {e}")

    async def on_reconciliation_alert(self, msg):
        try:
            data = json.loads(msg.data.decode())
            div = data.get("details", {})
            text = (
                f"⚖️ *DIVERGENCIA DE RECONCILIACIÓN*\n\n"
                f"📍 Broker: `{self._esc(str(div.get('broker', '?')))}`\n"
                f"📍 Symbol: `{self._esc(str(div.get('symbol', '?')))}`\n"
                f"📊 DB Qty: `{div.get('db', '?')}`\n"
                f"📈 Real Qty: `{div.get('real', '?')}`\n"
                f"❌ Diff: `{div.get('diff', '?')}`\n\n"
                f"🛑 Estrategias asociadas DETENIDAS\\."
            )
            await self.broadcast(text, level="critical")
        except Exception as e:
            logger.error(f"Error on_reconciliation_alert: {e}")

    async def on_balance_alert(self, msg):
        try:
            data = json.loads(msg.data.decode())
            await self.broadcast(
                f"💰 *Alerta de Balance*\n{self._esc(str(data))}",
                level="warning"
            )
        except Exception as e:
            logger.error(f"Error on_balance_alert: {e}")

    async def on_signal_alert(self, msg):
        """Señales de estrategias (Hurst, breakout, etc.)"""
        try:
            data = json.loads(msg.data.decode())
            side_emoji = "🟢" if data.get("side", "").upper() == "BUY" else "🔴"
            text = (
                f"{side_emoji} *SEÑAL DE TRADING*\n\n"
                f"📊 Estrategia: `{self._esc(data.get('strategy', '?'))}`\n"
                f"🏷️ Symbol: `{self._esc(data.get('symbol', '?'))}`\n"
                f"📈 Dirección: `{data.get('side', '?')}`\n"
                f"📐 Hurst: `{data.get('hurst', '?')}`\n"
                f"🎯 Qty: `{data.get('qty', '?')}`"
            )
            await self.broadcast(text, level="trade")
        except Exception as e:
            logger.error(f"Error on_signal_alert: {e}")

    async def on_order_event(self, msg):
        try:
            data = json.loads(msg.data.decode())
            event_type = data.get("event_type")

            if event_type == "ORDER_FILLED":
                side = data.get("side", "?").upper()
                side_emoji = "🟢" if side == "BUY" else "🔴"
                text = (
                    f"{side_emoji} *ORDEN EJECUTADA*\n\n"
                    f"🏷️ Symbol: `{self._esc(data.get('symbol', '?'))}`\n"
                    f"📈 Lado: `{side}`\n"
                    f"📦 Qty: `{data.get('qty', '?')}`\n"
                    f"💲 Precio: `{data.get('execution_price', '?')}`\n"
                    f"🏦 Broker: `{self._esc(data.get('broker', '?'))}`\n"
                    f"🆔 `{self._esc(str(data.get('broker_order_id', ''))[:12])}`"
                )
                await self.broadcast(text, level="trade")

            elif event_type == "ORDER_REJECTED":
                text = (
                    f"🚫 *ORDEN RECHAZADA*\n\n"
                    f"🏷️ Symbol: `{self._esc(data.get('symbol', '?'))}`\n"
                    f"📦 Orden: {data.get('side')} {data.get('qty')}\n"
                    f"❌ Razón: `{self._esc(str(data.get('payload', {}).get('error', 'desconocida')))}`"
                )
                await self.broadcast(text, level="warning")

            elif event_type in ("ORDER_ERROR", "EXECUTION_ERROR"):
                text = (
                    f"💥 *ERROR DE EJECUCIÓN*\n\n"
                    f"🏷️ Symbol: `{self._esc(data.get('symbol', '?'))}`\n"
                    f"❌ Error: `{self._esc(str(data.get('payload', {}).get('error', 'desconocido')))}`"
                )
                await self.broadcast(text, level="critical")

            elif event_type == "GLOBAL_PANIC":
                await self.broadcast(
                    "🔥 *MODO PÁNICO GLOBAL ACTIVADO* 🔥\n\n"
                    "Todas las estrategias están siendo detenidas\\.",
                    level="critical"
                )
        except Exception as e:
            logger.error(f"Error on_order_event: {e}")

    async def on_balance_reply(self, msg):
        pass  # Usado internamente por balance_monitor_loop

    @staticmethod
    def _esc(text: str) -> str:
        """Escapa caracteres especiales para MarkdownV2 de Telegram."""
        for char in r"_*[]()~`>#+-=|{}.!":
            text = text.replace(char, f"\\{char}")
        return text


if __name__ == "__main__":
    asyncio.run(AlertManager().start())
