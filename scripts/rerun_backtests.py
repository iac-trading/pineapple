import os
import json
import asyncio
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
from nats.aio.client import Client as NATS

# Configuration
DB = {
    "host": os.getenv("POSTGRES_HOST", "192.168.100.201"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "trading"),
    "user": os.getenv("POSTGRES_USER", "tsdb"),
    "password": os.environ["POSTGRES_PASSWORD"],
}
NATS_URL = os.environ["NATS_URL"]

async def rerun_all():
    # 1. Connect to DB and fetch previous backtests
    print("Fetching backtest history from database...")
    try:
        conn = psycopg2.connect(**DB)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT blueprint_id, broker, symbol, start_ts, end_ts, params, metrics->>'ts' as created_at
                FROM backtest_results
                ORDER BY metrics->>'ts' DESC
            """)
            jobs = cur.fetchall()
            
            if jobs:
                print(f"Cleaning up {len(jobs)} old records from database...")
                cur.execute("DELETE FROM backtest_results")
                conn.commit()
                print("Database cleared. Preparing fresh start.")
                
        conn.close()
    except Exception as e:
        print(f"Database error: {e}")
        return

    if not jobs:
        print("No previous backtests found in database.")
        return

    print(f"Found {len(jobs)} backtests in history.")

    # 2. Connect to NATS
    nc = NATS()
    try:
        await nc.connect(servers=[NATS_URL])
    except Exception as e:
        print(f"NATS connection error: {e}")
        return

    # 3. Resubmit each job
    print("\nResubmitting jobs (processing takes ~10s per job)...")
    for i, job in enumerate(jobs):
        new_job_id = str(uuid.uuid4())
        payload = {
            "job_id": new_job_id,
            "blueprint_id": job["blueprint_id"],
            "broker": job["broker"],
            "symbol": job["symbol"],
            "start_ts": job["start_ts"].isoformat() if job["start_ts"] else None,
            "end_ts": job["end_ts"].isoformat() if job["end_ts"] else None,
            "params": job["params"] or {}
        }
        
        await nc.publish("bt.request", json.dumps(payload).encode())
        
        # The backtester lab is on .203
        url = f"http://192.168.100.203:8081/reports/{new_job_id}.html"
        
        # Map blueprint to name for clarity
        names = {"101": "Donchian Breakout", "102": "Bollinger Reversion", "213": "Turtle Inst.", "19": "Pairs Trading", "301": "RSI Reversion", "302": "EMA Trend"}
        bp_name = names.get(str(job["blueprint_id"]), f"BP {job['blueprint_id']}")
        
        # Handle date from ISO string
        date_str = job["created_at"][:16].replace("T", " ") if job.get("created_at") else "N/A"
        
        print(f"[{i+1}/{len(jobs)}] {date_str} | {job['symbol']} | {bp_name}")
        print(f"      URL: {url}")

    await nc.close()
    print("\nAll backtests resubmitted successfully!")
    print("IMPORTANT: The reports are generated in the background. Please wait ~15-30 seconds before opening the links.")

    await nc.close()
    print("\nAll backtests resubmitted successfully!")
    print("The backtester service will process them and generate new reports with customized titles.")

if __name__ == "__main__":
    asyncio.run(rerun_all())
