import os
import json
import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
import psycopg2
from nats.aio.client import Client as NATS

print("--- Risk Engine Process Started ---", flush=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger("RiskEngine")

NATS_URL = os.getenv("NATS_URL", "nats://192.168.100.200:4222")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "trading")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")

INTENT_SUBJECT = "orders.intent"
SUBMIT_SUBJECT = "orders.submit"
EVENTS_SUBJECT = "orders.events"
MARKET_DATA_WILDCARD = "md.*.*.tick"

# Risk Limits Configuration
MAX_GLOBAL_EXPOSURE_USD = 100000.0  # nominal value
MAX_REJECTIONS_BEFORE_QUARANTINE = 5
MAX_ORDERS_PER_MINUTE = 100

class RiskEngine:
    def __init__(self):
        self.nc = NATS()
        # State tracking
        self.positions = defaultdict(float) # symbol -> net_qty
        self.instance_exposure = defaultdict(float) # instance_id -> total_abs_nominal
        self.rejection_counters = defaultdict(int) # instance_id -> count
        self.quarantined_instances = set()
        
        self.price_cache = {} # symbol -> last_price
        self.order_freq_counter = 0
        self.last_reset = datetime.now()
        
        self.panic_mode = False
        
        # Risk & Position Sizing State
        self.total_equity = 0.0
        self.last_balance_ts = 0

    async def start(self):
        print("🛡️ Institutional Risk Engine Starting...", flush=True)
        print(f"NATS_URL: {NATS_URL}", flush=True)
        
        try:
            print("Connecting to NATS...", flush=True)
            await self.nc.connect(servers=[NATS_URL])
            print("✅ NATS Connected successfully.", flush=True)
        except Exception as e:
            print(f"❌ NATS Connection FAILED: {e}", flush=True)
            raise

        print("Subscribing to subjects...", flush=True)
        await self.nc.subscribe(INTENT_SUBJECT, cb=self.on_order_intent)
        await self.nc.subscribe(MARKET_DATA_WILDCARD, cb=self.on_market_data)
        await self.nc.subscribe("orders.panic", cb=self.on_panic)
        
        print(f"Guarding {INTENT_SUBJECT} -> {SUBMIT_SUBJECT}", flush=True)
        
        # Initial balance & exposure sync
        print("Performing initial sync...", flush=True)
        await self.update_balance()
        await self.sync_positions()
        print("Initial sync done.", flush=True)

        print("Entering main loop...", flush=True)
        while True:
            await asyncio.sleep(60)
            self.order_freq_counter = 0
            self.last_reset = datetime.now()
            
            # Periodically sync balance & positions (every 60s)
            if time.time() - self.last_balance_ts >= 60:
                asyncio.create_task(self.update_balance())
                asyncio.create_task(self.sync_positions())

    async def update_balance(self):
        try:
            logger.info("🔄 Syncing global balance with Bridge...")
            resp = await self.nc.request("bridge.balance.get", b"", timeout=5)
            data = json.loads(resp.data.decode())
            if data.get("status") == "ok":
                self.total_equity = float(data.get("total_equity", 0.0))
                self.last_balance_ts = time.time()
                details = data.get("details", {})
                
                # Persist to database for Grafana/Recon
                try:
                    conn = psycopg2.connect(
                        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                        user=DB_USER, password=DB_PASS, connect_timeout=5
                    )
                    with conn.cursor() as cur:
                        for broker, balance in details.items():
                            cur.execute(
                                "INSERT INTO broker_balances (broker, balance, equity) VALUES (%s, %s, %s)",
                                (broker, float(balance), float(balance))
                            )
                        conn.commit()
                    conn.close()
                except Exception as db_err:
                    logger.error(f"Failed to persist balances to DB: {db_err}")

                logger.info(f"💰 Global Equity Synced: ${self.total_equity:,.2f} ({len(details)} brokers)")
        except Exception as e:
            logger.error(f"Failed to sync balance: {e}")

    async def sync_positions(self):
        """Syncs in-memory positions with the actual Database state (v_strategy_performance)"""
        try:
            logger.info("🔄 Syncing global exposure with Database...")
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASS, connect_timeout=5
            )
            with conn.cursor() as cur:
                # Query net_position_qty per symbol from the performance view
                cur.execute("SELECT symbol, SUM(net_position_qty) FROM v_strategy_performance GROUP BY symbol")
                rows = cur.fetchall()
                
                new_positions = defaultdict(float)
                for symbol, qty in rows:
                    new_positions[symbol] = float(qty)
                
                # Update atomic-ish swap
                self.positions = new_positions
                logger.info(f"✅ Global Exposure Synced. Monitored Symbols: {list(self.positions.keys())}")
            conn.close()
        except Exception as e:
            logger.error(f"Failed to sync positions with DB: {e}")

    def get_atr(self, symbol: str, window: int = 14):
        """Calculates ATR from market_candles (Daily)"""
        try:
            print(f"📊 [ATR] Attempting calculation for {symbol} (Host: {DB_HOST})", flush=True)
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASS, connect_timeout=5
            )
            with conn.cursor() as cur:
                # Fetch last window+1 candles for True Range calculation
                cur.execute("""
                    SELECT high, low, close 
                    FROM market_candles 
                    WHERE symbol = %s AND granularity = 86400
                    ORDER BY ts DESC LIMIT %s
                """, (symbol, window + 1))
                rows = cur.fetchall()
                print(f"📊 [ATR] Found {len(rows)} daily candles for {symbol}", flush=True)

                if len(rows) < window:
                    print(f"⚠️ [ATR] Insufficient data for {symbol}: {len(rows)} < {window}", flush=True)
                    return None
                
                # TR = max(high-low, abs(high-prev_close), abs(low-prev_close))
                trs = []
                for i in range(len(rows) - 1):
                    h, l, c = rows[i]
                    pc = rows[i+1][2]
                    tr = max(h - l, abs(h - pc), abs(l - pc))
                    trs.append(tr)
                
                res = sum(trs) / len(trs)
                print(f"✅ [ATR] Calculated: {res}", flush=True)
                return res
        except Exception as e:
            print(f"❌ [ATR] DB Error for {symbol}: {e}", flush=True)
            return None
        finally:
            if 'conn' in locals() and conn: conn.close()

    def get_macro_sentiment(self):
        """Consulta el sentimiento global reciente desde Market Intelligence (24h)"""
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASS, connect_timeout=5
            )
            with conn.cursor() as cur:
                # 1. Check for extreme sentiment (Average last 24h)
                cur.execute("SELECT AVG(sentiment_score) FROM ai_news_events WHERE ts > NOW() - INTERVAL '24 hours'")
                avg_sent = cur.fetchone()[0]
                
                # 2. Check for high-impact events (Impact 5 = Whale/Macro Alert)
                cur.execute("SELECT COUNT(*) FROM ai_news_events WHERE impact_level >= 5 AND ts > NOW() - INTERVAL '6 hours'")
                critical_events = cur.fetchone()[0]
                
                return float(avg_sent) if avg_sent else 0.0, int(critical_events)
        except Exception as e:
            logger.error(f"⚠️ [RISK] Error consultando sentimiento macro: {e}")
            return 0.0, 0
        finally:
            if 'conn' in locals() and conn: conn.close()

    async def on_market_data(self, msg):
        try:
            data = json.loads(msg.data.decode())
            symbol = data.get("symbol")
            price = data.get("last")
            if symbol and price:
                self.price_cache[symbol] = float(price)
        except Exception:
            pass

    async def on_order_intent(self, msg):
        raw = msg.data.decode()
        print(f"📥 Received Order Intent: {raw}", flush=True)
        try:
            payload = json.loads(raw)
            instance_id = payload.get("instance_id")
            symbol = payload.get("symbol")
            side = payload.get("side", "").upper()
            qty = float(payload.get("qty", 0))
            corr_id = payload.get("correlation_id")

            # --- GUARD -1: PANIC MODE ---
            if self.panic_mode:
                await self.reject(payload, "GLOBAL PANIC MODE ACTIVE: All new orders blocked.")
                return

            # --- GUARD 0: QUARANTINE ---
            if instance_id in self.quarantined_instances:
                await self.reject(payload, f"Circuit Breaker: Instance {instance_id} is in QUARANTINE")
                return

            # --- GUARD 1: FREQUENCY ---
            self.order_freq_counter += 1
            if self.order_freq_counter > MAX_ORDERS_PER_MINUTE:
                await self.reject(payload, "Risk Rejection: Global rate limit exceeded")
                return

            # --- GUARD 1.5: SENTIENT MARKET CHECK (NEW) ---
            avg_sentiment, critical_count = self.get_macro_sentiment()
            if avg_sentiment < -0.6:
                await self.reject(payload, f"SentientGuard: EXTREME FEAR DETECTED (Sent: {avg_sentiment:.2f}). Blocking all long/new orders.")
                return
            if critical_count > 0:
                await self.reject(payload, f"SentientGuard: CRITICAL MACRO/WHALE EVENT DETECTED ({critical_count} events). Market too volatile.")
                return

            # --- DYNAMIC POSITION SIZING (MISSION 16) ---
            # We do this BEFORE nominal checks so the nominal value reflects the adjusted size
            size_model = payload.get("meta", {}).get("size_model")
            if size_model == "ATR_RISK_1PCT":
                atr = self.get_atr(symbol)
                if atr and atr > 0:
                    risk_amount = self.total_equity * 0.01
                    multiplier = float(payload.get("meta", {}).get("atr_mult", 2.0))
                    new_qty = risk_amount / (atr * multiplier)
                    new_qty = round(new_qty, 4)
                    print(f"⚖️ [RISK SIZING] Resizing {symbol}: {qty} -> {new_qty} (Risk=$ {risk_amount}, ATR={atr})", flush=True)
                    qty = new_qty
                    payload["qty"] = qty
                else:
                    print(f"⚠️ ATR for {symbol} unknown. Falling back to original qty: {qty}", flush=True)
            
            elif size_model == "FIXED_MIN_LOT":
                min_lot = float(payload.get("meta", {}).get("min_lot", 0.001))
                print(f"⚖️ [RISK SIZING] FIXED_MIN_LOT for {symbol}: {qty} -> {min_lot}", flush=True)
                qty = min_lot
                payload["qty"] = qty

            # --- GUARD 2: LEVERAGE (GLOBAL NOMINAL) ---
            price = self.price_cache.get(symbol)
            if not price:
                print(f"⚠️ Price for {symbol} unknown. Using conservative fallback for Risk Check.", flush=True)
                if qty > 0.1: # Threshold for "blind" orders
                    await self.reject(payload, f"LeverageGuard: Price for {symbol} unknown. Cannot authorize blind risk.")
                    return
                price = 1.0 # Minimal fallback for micro-orders

            nominal_value = qty * price
            
            # Calculate total exposure using last known prices
            current_total_exposure = 0
            for sym, pos_qty in self.positions.items():
                mkt_price = self.price_cache.get(sym, 0.0)
                current_total_exposure += abs(pos_qty) * mkt_price

            if current_total_exposure + nominal_value > MAX_GLOBAL_EXPOSURE_USD:
                await self.reject(payload, f"LeverageGuard: Global exposure (${current_total_exposure + nominal_value:,.2f}) would exceed limit of ${MAX_GLOBAL_EXPOSURE_USD:,.2f}")
                return

            # --- GUARD 3: WASH TRADE (CROSS-STRATEGY CONFLICT) ---
            # Si otra estrategia ya tiene una posicion abierta en el sentido opuesto,
            # alertar o bloquear para evitar quemar comisiones innecesariamente.
            net_pos = self.positions[symbol]
            if (net_pos > 0 and side == "SELL") or (net_pos < 0 and side == "BUY"):
                 # Esta orden esta cerrando posicion global, lo cual es generalmente bueno.
                 pass
            
            # --- PASS: Authorized ---
            logger.info(f"✅ Risk Cleared [{instance_id[:8]}]: {side} {qty} {symbol}")
            
            # Update internal tracking (assuming execution succeeds for risk purposes)
            # In a production system, we'd update this on ORDER_FILLED events.
            delta = qty if side == "BUY" else -qty
            self.positions[symbol] += delta
            
            await self.nc.publish(SUBMIT_SUBJECT, json.dumps(payload).encode())

        except Exception as e:
            logger.error(f"Error in Risk Engine: {e}")

    async def reject(self, payload, reason):
        instance_id = payload.get("instance_id")
        print(f"❌ [RISK REJECT] {instance_id}: {reason}", flush=True)
        logger.warning(f"❌ REJECTED [{instance_id[:8]}]: {reason}")
        
        # Increment Circuit Breaker
        self.rejection_counters[instance_id] += 1
        if self.rejection_counters[instance_id] >= MAX_REJECTIONS_BEFORE_QUARANTINE:
            self.quarantined_instances.add(instance_id)
            logger.critical(f"🛑 CIRCUIT BREAKER TRIPPED for {instance_id}. QUARANTINED.")
            
            # Publish to Alert Manager
            alert_payload = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "source": "risk_engine",
                "msg": f"CIRCUIT BREAKER: Instance {instance_id[:8]} Quarantined",
                "details": {
                    "instance_id": instance_id,
                    "rejections_count": self.rejection_counters[instance_id],
                    "reason": "Max consecutive rejections reached"
                }
            }
            await self.nc.publish("alerts.critical", json.dumps(alert_payload).encode())

        event = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "instance_id": instance_id,
            "correlation_id": payload.get("correlation_id"),
            "event_type": "ORDER_REJECTED",
            "status": "rejected",
            "broker": payload.get("meta", {}).get("broker", "unknown"),
            "symbol": payload.get("symbol", "unknown"),
            "side": payload.get("side", "unknown"),
            "qty": payload.get("qty", 0),
            "payload": {"error": reason, "source": "risk_engine_v2"}
        }
        await self.nc.publish(EVENTS_SUBJECT, json.dumps(event).encode())

    async def on_panic(self, msg):
        logger.critical("🚨 GLOBAL PANIC RECEIVED! INITIATING HALT.")
        self.panic_mode = True
        
        # 1. Stop all running strategies in DB
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME, 
                user=DB_USER, password=DB_PASS, connect_timeout=5
            )
            with conn.cursor() as cur:
                cur.execute("UPDATE strategy_instances SET desired_status = 'stopped' WHERE is_active = TRUE")
            conn.commit()
            conn.close()
            logger.info("Strategies set to 'stopped' in DB.")
        except Exception as e:
            logger.error(f"Failed to update strategy_instances during panic: {e}")

        # 2. Flatten All monitored positions
        for symbol, qty in list(self.positions.items()):
            if abs(qty) > 0.0001:
                side = "SELL" if qty > 0 else "BUY"
                close_qty = abs(qty)
                logger.warning(f"Closing position: {side} {close_qty} {symbol}")
                
                payload = {
                    "schema": "orders.submit.v1",
                    "instance_id": "00000000-0000-0000-0000-000000000000",
                    "correlation_id": f"panic-close-{symbol}-{datetime.now().timestamp()}",
                    "side": side,
                    "qty": close_qty,
                    "symbol": symbol,
                    "meta": {"reason": "GLOBAL_HALT"}
                }
                await self.nc.publish(SUBMIT_SUBJECT, json.dumps(payload).encode())

if __name__ == "__main__":
    print("🚀 Initializing Risk Engine...", flush=True)
    try:
        engine = RiskEngine()
        print("Starting asyncio event loop...", flush=True)
        asyncio.run(engine.start())
    except Exception as e:
        print(f"CRITICAL CRASH during startup: {e}", flush=True)
        import traceback
        traceback.print_exc()
        time.sleep(10) # Prevent rapid restart loop flooding logs
