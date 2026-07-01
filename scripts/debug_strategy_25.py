import os
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone

from airflow.providers.postgres.hooks.postgres import PostgresHook

def main():
    try:
        hook = PostgresHook(postgres_conn_id='TRADING_DB')
        conn = hook.get_conn()
    except Exception as e:
        print(f"PostgresHook failed ({e}), trying manual fallback...")
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "192.168.100.201"),
            port=5432,
            dbname="trading",
            user="trading_user",
            password=os.environ["POSTGRES_PASSWORD"]
        )

    symbol = "TSLA"
    start_ts = "2025-03-01"
    end_ts = "2025-03-10"

    print(f"--- DIAGNOSTIC: Strategy 25 ({symbol}) ---")

    # 1. Check Candles
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT ts, close FROM market_candles WHERE symbol=%s AND granularity=86400 AND ts>=%s AND ts<=%s ORDER BY ts",
            (symbol, start_ts, end_ts)
        )
        candles = cur.fetchall()
    
    print(f"Found {len(candles)} daily candles.")
    if candles:
        df_px = pd.DataFrame(candles)
        df_px['ts'] = pd.to_datetime(df_px['ts']).dt.tz_localize(None)
        print("Sample candles:\n", df_px.head())

    # 2. Check Signals
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT ts, ofi, sentiment, vpin FROM intelligence_signals WHERE symbol=%s AND ts>=%s AND ts<=%s ORDER BY ts",
            (symbol, start_ts, end_ts)
        )
        signals = cur.fetchall()
    
    print(f"\nFound {len(signals)} micro-signals.")
    if signals:
        df_sig = pd.DataFrame(signals)
        df_sig['ts'] = pd.to_datetime(df_sig['ts']).dt.tz_localize(None)
        print("Sample signals:\n", df_sig.head())
        print("Max OFI:", df_sig['ofi'].max())

    # 3. Simulate Join
    if candles and signals:
        df_px.sort_values("ts", inplace=True)
        df_sig.sort_values("ts", inplace=True)
        
        df_joined = pd.merge_asof(df_px, df_sig, on="ts", direction="backward")
        print(f"\nJoined DF size: {len(df_joined)}")
        
        # Check logic
        ofi_threshold = 50
        sent_threshold = 0.2
        vpin_limit = 0.8
        
        df_joined['signal'] = 0
        df_joined.loc[(df_joined['ofi'] > ofi_threshold) & (df_joined['sentiment'] > sent_threshold) & (df_joined['vpin'] < vpin_limit), 'signal'] = 1
        
        sig_count = (df_joined['signal'] != 0).sum()
        print(f"Signals with current thresholds: {sig_count}")
        
        if sig_count > 0:
            print("\nWinning Signal Rows:")
            print(df_joined[df_joined['signal'] != 0])
        else:
            print("\nNo signal match found in simulation.")
            print("Check first row of join data for clues:")
            print(df_joined.head(3))

    conn.close()

if __name__ == "__main__":
    main()
