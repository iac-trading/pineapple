import os
import json
import asyncio
import logging
from datetime import datetime, timezone
import psycopg2
from nats.aio.client import Client as NATS

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] ReconciliationHub: %(message)s')
logger = logging.getLogger("ReconciliationHub")

NATS_URL = os.getenv("NATS_URL", "nats://192.168.100.200:4222")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "trading")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "600")) # 10 minutes
TOLERANCE = float(os.getenv("RECON_TOLERANCE", "0.0001"))

class ReconciliationHub:
    def __init__(self):
        self.nc = NATS()

    async def start(self):
        await self.nc.connect(servers=[NATS_URL])
        logger.info(f"⚖️ Reconciliation Hub Online. Interval: {CHECK_INTERVAL}s | Tolerance: {TOLERANCE}")
        
        while True:
            try:
                await self.reconcile()
            except Exception as e:
                logger.error(f"Reconciliation error: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

    def get_db_instances(self):
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS, connect_timeout=5
        )
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT instance_id, symbol, broker, qty, status, name
                    FROM strategy_instances
                    WHERE is_active = TRUE AND status = 'running'
                """)
                rows = cur.fetchall()
                db_state = {} # (broker, symbol) -> total_qty
                instances_info = {} # (broker, symbol) -> [instance_ids]
                for row in rows:
                    key = (row[2], row[1])
                    db_state[key] = db_state.get(key, 0.0) + float(row[3])
                    if key not in instances_info: instances_info[key] = []
                    instances_info[key].append(str(row[0]))
                return db_state, instances_info
        finally:
            conn.close()

    def halt_instances(self, instance_ids):
        """Emergency stop of strategies in the database"""
        if not instance_ids: return
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS, connect_timeout=5
        )
        try:
            with conn.cursor() as cur:
                query = "UPDATE strategy_instances SET desired_status = 'stopped' WHERE instance_id = ANY(%s::uuid[])"
                cur.execute(query, (instance_ids,))
                conn.commit()
                logger.critical(f"🛑 HALTED {len(instance_ids)} instances in DB.")
        finally:
            conn.close()

    async def reconcile(self):
        logger.info("🔍 Starting reconciliation cycle...")
        try:
            db_state, instances_info = self.get_db_instances()
        except Exception as e:
            logger.error(f"Failed to fetch DB instances: {e}")
            return
        
        try:
            msg = await self.nc.request("bridge.positions.get", b'{"broker": "all"}', timeout=15)
            resp = json.loads(msg.data.decode())
            if resp.get("status") != "ok":
                raise RuntimeError(f"Bridge error: {resp.get('message')}")
            broker_positions = resp.get("positions", [])
        except Exception as e:
            logger.error(f"Failed to fetch positions from bridge: {e}")
            return

        broker_state = {} 
        for pos in broker_positions:
            key = (pos['broker'], pos['symbol'])
            broker_state[key] = broker_state.get(key, 0.0) + float(pos['qty'])

        all_keys = set(db_state.keys()) | set(broker_state.keys())
        divergences = []
        for key in all_keys:
            broker, symbol = key
            db_qty = db_state.get(key, 0.0)
            real_qty = broker_state.get(key, 0.0)
            
            diff = abs(db_qty - real_qty)
            if diff > TOLERANCE: 
                divergences.append({
                    "broker": broker, "symbol": symbol, 
                    "db": db_qty, "real": real_qty, "diff": diff,
                    "instances": instances_info.get(key, [])
                })

        if not divergences:
            logger.info("✅ Reconciliation complete: All states match.")
            return

        for div in divergences:
            logger.critical(f"🚨 DIVERGENCE: {div['broker']}:{div['symbol']} | DB={div['db']} | Real={div['real']} | Diff={div['diff']}")
            
            # 1. Halt associated strategies
            self.halt_instances(div['instances'])
            
            # 2. Send Alerts
            await self.send_alerts(div)

    async def send_alerts(self, div):
        # NATS Internal Alert (Now centralized via Alert Manager)
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": "CRITICAL",
            "type": "RECONCILIATION_DIVERGENCE",
            "msg": f"CRITICAL STATE DIVERGENCE: {div['broker']}:{div['symbol']}",
            "details": div
        }
        await self.nc.publish("alerts.reconciliation", json.dumps(payload).encode())
        logger.info(f"📤 Alert published to NATS for {div['broker']}:{div['symbol']}")

if __name__ == "__main__":
    asyncio.run(ReconciliationHub().start())
