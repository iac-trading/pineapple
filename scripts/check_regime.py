import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json

DB = {
    "host": os.getenv("POSTGRES_HOST", "192.168.100.201"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "trading"),
    "user": os.getenv("POSTGRES_USER", "tsdb"),
    "password": os.environ["POSTGRES_PASSWORD"],
}

def check_regime():
    print("=== AXIO-QUANT | REGIME MONITOR ===")
    try:
        conn = psycopg2.connect(**DB)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("SELECT ts, regime_label, confidence, meta FROM market_regimes ORDER BY ts DESC LIMIT 5")
        rows = cur.fetchall()
        
        if not rows:
            print("No regimes found in database.")
            return

        for row in rows:
            print(f"[{row['ts']}] Regime: {row['regime_label']} | Conf: {row['confidence']:.2f}")
            meta = row['meta']
            if 'features' in meta:
                print(f"  Features used: {meta['features']}")
            print("-" * 40)
            
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error connecting to DB: {e}")

if __name__ == "__main__":
    check_regime()
