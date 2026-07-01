import yfinance as yf
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import pandas as pd
import pytz

# Config
SYMBOLS = ["AAPL", "NVDA", "TSLA", "META", "AMZN", "MSFT", "GOOGL"]
START_DATE = "2020-01-01"
END_DATE = "2026-03-01"
BROKER = "yfinance"

DB_CONN_ID = "TRADING_DB"

def main():
    print(f"Ingesting Daily Candles from {START_DATE} for {len(SYMBOLS)} symbols via Yahoo Finance...")

    # Use Airflow PostgresHook securely
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    total_rows = 0

    for sym in SYMBOLS:
        print(f"Fetch {sym}...")
        try:
            df = yf.download(sym, start=START_DATE, end=END_DATE, interval="1d", progress=False)
            if df.empty:
                print(f"No data for {sym}")
                continue
                
            if isinstance(df.columns, pd.MultiIndex):
                 df.columns = df.columns.droplevel(1)
            
            df = df[['Open', 'High', 'Low', 'Close', 'Volume']].dropna()
            
            records = []
            for ts, row in df.iterrows():
                 dt_utc = ts.tz_convert('UTC') if ts.tzinfo else ts.replace(tzinfo=pytz.UTC)
                 # 86400 is the integer number of seconds for 1 day
                 records.append((
                     dt_utc, sym, BROKER, 86400, 
                     float(row['Open']), float(row['High']), float(row['Low']), float(row['Close']), float(row['Volume'])
                 ))
                 
            execute_values(
                cur,
                """
                INSERT INTO market_candles (ts, symbol, broker, granularity, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT (broker, symbol, granularity, ts) 
                DO UPDATE SET 
                    open=EXCLUDED.open,
                    high=EXCLUDED.high,
                    low=EXCLUDED.low,
                    close=EXCLUDED.close,
                    volume=EXCLUDED.volume
                """,
                records
            )
            conn.commit()
            print(f"Saved {len(records)} daily candles for {sym}")
            total_rows += len(records)
        except Exception as e:
            print(f"Error fetching {sym}: {e}")

    print(f"SUCCESS: Inserted/Updated {total_rows} total candles.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
