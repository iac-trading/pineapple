import os, json, asyncio, logging
from datetime import datetime, timezone
import uuid

import psycopg2
import redis
from nats.aio.client import Client as NATS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BridgeExecutor")

def utc_now():
    return datetime.now(timezone.utc)

class BridgeExecutor:
    """
    Consumes:  NATS subject  orders.submit
    Expects:   schema orders.submit.v1 with instance_id (UUID)
    Writes:    orders table (V3)
    """
    def __init__(self):
        self.nc = NATS()

        # Redis for idempotency (avoid double orders)
        self.redis = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"))

        self.nats_url = os.getenv("NATS_URL", "nats://nats:4222")

        # Postgres canonical env names (fallback for legacy POSTGRES_PASS)
        self.db_params = {
            "host": os.getenv("POSTGRES_HOST", "postgres"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "dbname": os.getenv("POSTGRES_DB", "platform"),
            "user": os.getenv("POSTGRES_USER", "platform"),
            "password": os.getenv("POSTGRES_PASSWORD") or os.getenv("POSTGRES_PASS") or "",
            "connect_timeout": 5,
        }

    async def run(self):
        await self.nc.connect(servers=[self.nats_url])
        await self.nc.subscribe("orders.submit", cb=self.on_order_received)
        logger.info("🌉 Bridge Executor Online (V3) - Listening: orders.submit")
        while True:
            await asyncio.sleep(1)

    def _insert_order(self, payload: dict, result: dict):
        conn = psycopg2.connect(**self.db_params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO orders (
                        ts, instance_id, correlation_id, broker, symbol, side, qty, price, status, broker_order_id, raw
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        utc_now(),
                        payload["instance_id"],
                        payload["correlation_id"],
                        result.get("broker", payload.get("broker", "paper")),
                        payload["symbol"],
                        payload["side"],
                        float(payload["qty"]),
                        float(result.get("price") or 0.0),
                        result.get("status", "filled"),
                        result.get("broker_order_id"),
                        json.dumps({"request": payload, "result": result}),
                    ),
                )
            conn.commit()
        finally:
            conn.close()

    async def on_order_received(self, msg):
        try:
            payload = json.loads(msg.data.decode())
        except Exception:
            logger.exception("Invalid JSON payload in orders.submit")
            return

        schema = payload.get("schema")
        if schema and schema != "orders.submit.v1":
            logger.warning(f"Unknown schema={schema} (expected orders.submit.v1). Processing anyway.")

        # Required fields (V3)
        if not payload.get("instance_id"):
            logger.error("Missing instance_id (V3 canonical). Rejecting order.")
            return

        # Normalize instance_id to UUID string
        try:
            payload["instance_id"] = str(uuid.UUID(str(payload["instance_id"])))
        except Exception:
            logger.error("instance_id is not a valid UUID. Rejecting order.")
            return

        payload.setdefault("correlation_id", str(uuid.uuid4()))
        payload.setdefault("broker", "paper")

        for k in ("symbol", "side", "qty"):
            if k not in payload:
                logger.error(f"Missing required field: {k}. Rejecting order.")
                return

        corr_id = payload["correlation_id"]

        # Idempotency lock (30s)
        if not self.redis.set(f"exec_lock:{corr_id}", "1", nx=True, ex=30):
            logger.warning(f"Duplicate order ignored correlation_id={corr_id}")
            return

        logger.info(
            f"🚀 Executing {payload['side']} qty={payload['qty']} symbol={payload['symbol']} "
            f"instance_id={payload['instance_id']} corr={corr_id}"
        )

        # TODO: Real broker execution (Deriv/IBKR)
        result = {
            "status": "filled",
            "price": 0.0,
            "broker_order_id": f"sim-{corr_id[:8]}",
            "broker": payload.get("broker", "paper"),
        }

        try:
            self._insert_order(payload, result)
        except Exception as e:
            logger.error(f"DB Error inserting order corr={corr_id}: {e}")

if __name__ == "__main__":
    asyncio.run(BridgeExecutor().run())