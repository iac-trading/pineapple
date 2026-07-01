import os, json, asyncio, logging, time
from datetime import datetime, timezone
from uuid import UUID
import psycopg2
import redis
from aiohttp import web
from nats.aio.client import Client as NATS

from .deriv import deriv_ticks_stream
from .deriv_executor import DerivExecutor
from .ccxt_client import CcxtClient
from .ibkr import IbkrClient
from .models import OrderSubmit, OrderEvent

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("BridgeEMS")

NATS_URL = os.getenv("NATS_URL")
REDIS_URL = os.getenv("REDIS_URL")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "platform")
POSTGRES_USER = os.getenv("POSTGRES_USER", "platform")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

DERIV_APP_ID = os.getenv("DERIV_APP_ID", "")
DERIV_TOKEN = os.getenv("DERIV_TOKEN", "")
DERIV_SYMBOLS = os.getenv("DERIV_SYMBOLS", "R_75") 

IBKR_HOST = os.getenv("IBKR_HOST", "192.168.100.202")
IBKR_PORT = int(os.getenv("IBKR_PORT", "4001"))

HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8889"))

SUBMIT_SUBJECT = "orders.submit"
EVENTS_SUBJECT = "orders.events"
TICKS_PREFIX = "md"

def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()

def pg_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=5
    )

async def http_health_server():
    async def health(_):
        return web.json_response({"status": "ok", "ts": utc_now_iso()})
    app = web.Application()
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HEALTH_PORT)
    await site.start()

class Bridge:
    def __init__(self):
        self.nc = NATS()
        self.rds = redis.from_url(REDIS_URL) if REDIS_URL else None
        
        # Broker Clients (Lazy loading)
        self.deriv_exec = DerivExecutor(DERIV_APP_ID, DERIV_TOKEN) if DERIV_APP_ID and DERIV_TOKEN else None
        self.ibkr_execs = {} # Map of broker_id -> IbkrClient
        self.ccxt_execs = {} # Map of exchange_id -> CcxtClient

    def get_ccxt_client(self, exchange_id: str):
        if exchange_id not in self.ccxt_execs:
            self.ccxt_execs[exchange_id] = CcxtClient(exchange_id)
        return self.ccxt_execs[exchange_id]

    def get_ibkr_client(self, broker_id: str):
        if broker_id not in self.ibkr_execs:
            self.ibkr_execs[broker_id] = IbkrClient(IBKR_HOST, broker_id)
        return self.ibkr_execs[broker_id]

    async def start(self):
        if not NATS_URL:
            raise RuntimeError("NATS_URL missing")
        await self.nc.connect(servers=[NATS_URL])
        await self.nc.subscribe(SUBMIT_SUBJECT, cb=self.on_order_submit)
        await self.nc.subscribe("bridge.positions.get", cb=self.on_fetch_positions)
        await self.nc.subscribe("bridge.balance.get", cb=self.on_get_balance)
        logger.info(f"🚀 Bridge EMS Online. Listening {SUBMIT_SUBJECT}, bridge.positions.get & bridge.balance.get")
        logger.info("Bridge Provider: Rate limiter initialized.")
        await http_health_server()

        tasks = []
        if DERIV_APP_ID and DERIV_TOKEN:
            for sym in [s.strip() for s in DERIV_SYMBOLS.split(",") if s.strip()]:
                tasks.append(asyncio.create_task(self.run_deriv_sensor(sym)))

        if tasks:
            await asyncio.gather(*tasks)
        else:
            while True:
                await asyncio.sleep(5)

    async def run_deriv_sensor(self, symbol: str):
        logger.info(f"Deriv sensor enabled for {symbol}")
        while True:
            try:
                async for tick in deriv_ticks_stream(DERIV_APP_ID, DERIV_TOKEN, symbol):
                    subject = f"{TICKS_PREFIX}.deriv.{symbol.replace(' ', '_')}.tick"
                    await self.nc.publish(subject, json.dumps(tick).encode())
            except Exception as e:
                logger.error(f"Deriv sensor error for {symbol}: {e}")
                await asyncio.sleep(10)

    def fetch_instance(self, instance_id: UUID):
        conn = pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT instance_id, blueprint_id, name, owner, assigned_host,
                           symbol, broker, status, qty, params, is_active, is_shadow
                    FROM strategy_instances
                    WHERE instance_id = %s
                """, (str(instance_id),))
                row = cur.fetchone()
                if not row: return None
                return {
                    "instance_id": row[0], "blueprint_id": row[1], "name": row[2],
                    "owner_id": row[3], "assigned_host": row[4], "symbol": row[5],
                    "broker": row[6], "status": row[7],
                    "qty": row[8], "params": row[9] or {},
                    "is_active": row[10], "is_shadow": row[11],
                }
        finally:
            conn.close()

    def journal(self, instance_id: UUID, correlation_id: UUID, event_type: str, payload: dict, actor: str):
        conn = pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO journal_events (instance_id, correlation_id, event_type, payload, actor)
                    VALUES (%s, %s, %s, %s::jsonb, %s)
                """, (str(instance_id), str(correlation_id), event_type, json.dumps(payload), actor))
                conn.commit()
        finally:
            conn.close()

    def insert_order(self, evt: OrderEvent):
        conn = pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO orders (
                      ts, correlation_id, instance_id, broker, symbol, side, qty,
                      price, status, is_shadow, broker_order_id, raw
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                """, (
                    evt.ts,
                    str(evt.correlation_id),
                    str(evt.instance_id),
                    evt.broker,
                    evt.symbol,
                    evt.side,
                    evt.qty,
                    evt.execution_price,
                    evt.status,
                    evt.payload.get("is_shadow", False),
                    evt.broker_order_id,
                    json.dumps(evt.payload)
                ))
                conn.commit()
        finally:
            conn.close()

    async def publish_event(self, evt: OrderEvent):
        await self.nc.publish(EVENTS_SUBJECT, json.dumps(evt.model_dump()).encode())

    async def on_fetch_positions(self, msg):
        try:
            payload = json.loads(msg.data.decode()) if msg.data else {}
            target_broker = payload.get("broker", "all")
            
            positions = []
            
            # 1. Fetch from Deriv
            if target_broker in ("all", "deriv", "deriv_paper", "deriv_live") and self.deriv_exec:
                try:
                    positions.extend(await self.deriv_exec.get_positions())
                except Exception as e:
                    logger.error(f"Error fetching Deriv positions: {e}")

            # 2. Fetch from CCXT exchanges
            if target_broker == "all":
                for broker_id, client in self.ccxt_execs.items():
                    try:
                        positions.extend(await client.get_positions())
                    except Exception as e:
                        logger.error(f"Error fetching {broker_id} positions: {e}")
            elif target_broker in ("binance", "bybit", "okx", "kraken"):
                client = self.get_ccxt_client(target_broker)
                positions.extend(await client.get_positions())

            # 3. Fetch from IBKR
            if target_broker in ("all", "ibkr", "ibkr_paper", "ibkr_live"):
                target_ibkrs = ["ibkr_paper", "ibkr_live"] if target_broker in ("all", "ibkr") else [target_broker]
                for b_id in target_ibkrs:
                    try:
                        client = self.get_ibkr_client(b_id)
                        positions.extend(await client.get_positions())
                    except Exception as e:
                        logger.error(f"Error fetching {b_id} positions: {e}")

            if msg.reply:
                await self.nc.publish(msg.reply, json.dumps({"status": "ok", "positions": positions}).encode())
                
        except Exception as e:
            logger.error(f"Error in on_fetch_positions: {e}")
            if msg.reply:
                await self.nc.publish(msg.reply, json.dumps({"status": "error", "message": str(e)}).encode())

    async def on_get_balance(self, msg):
        try:
            total_equity = 0.0 # Starting from 0.0 to reflect real account balances
            details = {}

            # 1. Deriv
            try:
                if self.deriv_exec:
                    bal = await self.deriv_exec.get_balance()
                    total_equity += bal["total"]
                    details["deriv"] = bal["total"]
            except Exception as e:
                logger.error(f"Error fetching Deriv balance: {e}")

            # 2. CCXT
            for exchange_id, client in self.ccxt_execs.items():
                try:
                    balance = await client.exchange.fetch_balance()
                    equity = float(balance.get('total', {}).get('USD', 0.0))
                    if equity == 0: # Try USDT
                        equity = float(balance.get('total', {}).get('USDT', 0.0))
                    total_equity += equity
                    details[exchange_id] = equity
                except Exception as e:
                    logger.error(f"Error fetching CCXT balance for {exchange_id}: {e}")

            # 3. IBKR
            for b_id, client in self.ibkr_execs.items():
                try:
                    if client.ib.isConnected():
                        acc_vals = client.ib.accountValues()
                        net_liquidation = next((v.value for v in acc_vals if v.tag == 'NetLiquidation' and v.currency == 'USD'), 0)
                        total_equity += float(net_liquidation)
                        details[b_id] = float(net_liquidation)
                except Exception as e:
                    logger.error(f"Error fetching {b_id} balance: {e}")

            if msg.reply:
                await self.nc.publish(msg.reply, json.dumps({
                    "status": "ok",
                    "total_equity": total_equity,
                    "details": details,
                    "ts": utc_now_iso()
                }).encode())

        except Exception as e:
            logger.error(f"Error in on_get_balance: {e}")
            if msg.reply:
                await self.nc.publish(msg.reply, json.dumps({"status": "error", "message": str(e)}).encode())

    async def on_order_submit(self, msg):
        raw = msg.data.decode()
        try:
            payload = json.loads(raw)
            # Filter extra keys like 'schema' to avoid dataclass init error
            from dataclasses import fields
            valid_keys = {f.name for f in fields(OrderSubmit)}
            filtered_payload = {k: v for k, v in payload.items() if k in valid_keys}
            order = OrderSubmit(**filtered_payload)
        except Exception as e:
            logger.error(f"Invalid order submit: {e}")
            return

        correlation_id = order.correlation_id or UUID(int=0)
        
        # Dedup
        if self.rds:
            key = f"dedup:{correlation_id}"
            if not self.rds.set(key, "1", nx=True, ex=15):
                logger.warning(f"Duplicate ignored {correlation_id}")
                return

        inst = self.fetch_instance(order.instance_id)
        if not inst or not inst["is_active"]:
            logger.error(f"Instance {order.instance_id} not found or inactive.")
            self.journal(order.instance_id, correlation_id, "ERROR", {"error": "instance not found/inactive", "raw": payload}, "bridge")
            return

        self.journal(order.instance_id, correlation_id, "ORDER_SUBMITTED", payload, "strategy")

        broker = order.meta.get("broker") or inst["broker"]
        symbol = order.symbol or inst["symbol"]
        is_shadow = inst.get("is_shadow", False) or order.meta.get("is_shadow", False)

        # Shadow or Paper fill simulation
        if is_shadow or broker == "paper":
            status = "shadow_filled" if is_shadow else "filled"
            px = float(order.meta.get("price") or 0.0)
            
            evt = OrderEvent(
                ts=utc_now_iso(),
                instance_id=order.instance_id,
                correlation_id=correlation_id,
                event_type="ORDER_FILLED" if not is_shadow else "SHADOW_FILLED",
                status=status,
                broker=broker,
                symbol=symbol,
                side=order.side,
                qty=order.qty,
                execution_price=px,
                broker_order_id="shadow-sim" if is_shadow else "paper-sim",
                payload={"mode": "shadow" if is_shadow else "paper", "is_shadow": is_shadow}
            )
            await self._finalize_order(evt)
            return

        # LIVE EXECUTION (The EMS Heart)
        logger.info(f"🌐 EMS LIVE Routing: {broker} | {symbol} | {order.side} | {order.qty}")
        t0 = time.time()
        try:
            result = None
            if "deriv" in broker:
                if not self.deriv_exec: raise RuntimeError("Deriv Executor not configured")
                result = await self.deriv_exec.place_market_order(symbol, order.side, order.qty)
            
            elif broker in ("binance", "bybit", "okx", "kraken"):
                client = self.get_ccxt_client(broker)
                result = await client.place_order(symbol, order.side, order.qty)

            elif "ibkr" in broker:
                client = self.get_ibkr_client(broker)
                result = await client.place_market_order(symbol, order.side, order.qty)
            
            else:
                raise ValueError(f"Unsupported broker: {broker}")

            latency_ms = (time.time() - t0) * 1000
            result["latency_ms"] = latency_ms
            
            evt = OrderEvent(
                ts=utc_now_iso(),
                instance_id=order.instance_id,
                correlation_id=correlation_id,
                event_type="ORDER_FILLED",
                status=result["status"],
                broker=broker,
                symbol=symbol,
                side=order.side,
                qty=order.qty,
                execution_price=result.get("price"),
                broker_order_id=result.get("broker_order_id"),
                payload=result
            )
            await self._finalize_order(evt)

        except Exception as e:
            logger.exception(f"Execution Error for {broker} symbol={symbol}: {e}")
            evt = OrderEvent(
                ts=utc_now_iso(),
                instance_id=order.instance_id,
                correlation_id=correlation_id,
                event_type="ORDER_ERROR",
                status="error",
                broker=broker,
                symbol=symbol,
                side=order.side,
                qty=order.qty,
                payload={"error": str(e), "mode": "live"}
            )
            await self._finalize_order(evt)

    async def _finalize_order(self, evt: OrderEvent):
        self.insert_order(evt)
        self.journal(evt.instance_id, evt.correlation_id, evt.event_type, evt.model_dump(), "bridge")
        await self.publish_event(evt)
        logger.info(f"✅ Order Finalized: {evt.status} | broker={evt.broker} | id={evt.broker_order_id}")

if __name__ == "__main__":
    asyncio.run(Bridge().start())