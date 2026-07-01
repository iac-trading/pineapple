import os
import psycopg2
from psycopg2.extras import RealDictCursor

def check_vix_data():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "192.168.100.201"),
        port=5432,
        dbname="trading_db",
        user="trader",
        password=os.environ["POSTGRES_PASSWORD"]
    )
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        symbols = ["VX=F", "VX=F_NEXT", "^VIX", "SPY"]
        for sym in symbols:
            cur.execute("SELECT count(*) FROM market_candles WHERE symbol=%s", (sym,))
            cnt = cur.fetchone()["count"]
            print(f"Symbol {sym}: {cnt} rows")
            
            if cnt > 0:
                cur.execute("SELECT ts, close FROM market_candles WHERE symbol=%s ORDER BY ts DESC LIMIT 1", (sym,))
                last = cur.fetchone()
                print(f"  Latest {sym}: {last['ts']} @ {last['close']}")

    conn.close()

if __name__ == "__main__":
    check_vix_data()
