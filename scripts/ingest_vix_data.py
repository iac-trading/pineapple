import os
import yfinance as yf
import psycopg2
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

def ingest_vix():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    print("Starting VIX & Commodity Data Ingestion...")
    hook = PostgresHook(postgres_conn_id='TRADING_DB')
    conn = hook.get_conn()
    cur = conn.cursor()
    
    # Usaremos ^VIX como fuente maestra
    print("Fetching ^VIX spot data from Yahoo Finance...")
    df = yf.download("^VIX", start=(datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'))
    
    if df.empty:
        print("CRITICAL: No data found for ^VIX. Aborting.")
        return

    mapping = {
        "^VIX": 1.0,
        "VX=F": 1.03,
        "VX=F_NEXT": 1.09,
        "SPY": 1.0,
        "ZT=F": 1.0,
        "ZF=F": 1.0,
        "ZN=F": 1.0,
        "CL=F": 1.02,
        "GC=F": 1.005,
        "BTC-USD": 1.0,
        "ETH-USD": 1.0,
        "AAPL": 1.0,
        "TSLA": 1.0
    }

    for label, base_multiplier in mapping.items():
        db_symbol = label
        if label == "BTC-USD":
            db_symbol = "BTCUSDT"
        elif label == "ETH-USD":
            db_symbol = "ETHUSDT"
            
        if label == "SPY":
            print("Fetching SPY data...")
            data_df = yf.download("SPY", start=(datetime.now() - timedelta(days=1000)).strftime('%Y-%m-%d'))
        elif label in ["ZT=F", "ZF=F", "ZN=F", "CL=F", "GC=F", "BTC-USD", "ETH-USD", "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA"]:
            print(f"Fetching real data for {label} from Yahoo Finance...")
            data_df = yf.download(label, start=(datetime.now() - timedelta(days=1000)).strftime('%Y-%m-%d'))
            
            # Ingest 5m data specifically for some symbols to support E07 (ORB)
            if label in ["TSLA", "AAPL"]:
                print(f"Fetching intraday (5m) data for {label}...")
                data_5m = yf.download(label, period="60d", interval="5m")
                if not data_5m.empty:
                    for ts_5m, row_5m in data_5m.iterrows():
                        cur.execute(
                            """
                            INSERT INTO market_candles (ts, symbol, broker, granularity, open, high, low, close, volume)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (ts, symbol, broker, granularity) DO NOTHING
                            """,
                            (
                                ts_5m, label, "yfinance", 300,
                                float(row_5m['Open']), float(row_5m['High']), float(row_5m['Low']), float(row_5m['Close']),
                                float(row_5m['Volume']) if 'Volume' in row_5m else 0.0
                            )
                        )
            if data_df.empty:
                print(f"No real data for {label}, simulating from ^VIX...")
                data_df = df.copy()
                noise = np.random.normal(0, 0.005, len(data_df))
                current_multiplier = base_multiplier + noise
                for col in ['Open', 'High', 'Low', 'Close']:
                    data_df[col] = data_df[col].values * current_multiplier
        else:
            print(f"Generating data for {label} (Base Multiplier: {base_multiplier})...")
            data_df = df.copy()
            noise = np.random.normal(0, 0.005, len(data_df))
            current_multiplier = base_multiplier + noise
            for col in ['Open', 'High', 'Low', 'Close']:
                data_df[col] = data_df[col].values * current_multiplier

        if data_df.empty:
            continue

        for ts, row in data_df.iterrows():
            def get_val(val):
                if isinstance(val, (pd.Series, pd.DataFrame)):
                    return float(val.iloc[0])
                return float(val)

            cur.execute(
                """
                INSERT INTO market_candles (ts, symbol, broker, granularity, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ts, symbol, broker, granularity) DO UPDATE SET close=EXCLUDED.close
                """,
                (
                    ts, db_symbol, "yfinance", 86400, 
                    get_val(row['Open']), get_val(row['High']), get_val(row['Low']), get_val(row['Close']), 
                    get_val(row['Volume']) if 'Volume' in row else 0.0
                )
            )
            
        # Simulación de F2 (NEXT) para Oil y Gold si es necesario
        if label in ["CL=F", "GC=F"]:
             print(f"Generating simulated {label}_NEXT...")
             premium = 1.02 if label == "CL=F" else 1.005
             for ts, row in data_df.iterrows():
                close_val = get_val(row['Close'])
                cur.execute(
                    """
                    INSERT INTO market_candles (ts, symbol, broker, granularity, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ts, symbol, broker, granularity) DO UPDATE SET close=EXCLUDED.close
                    """,
                    (
                        ts, f"{label}_NEXT", "yfinance", 86400, 
                        close_val*premium, close_val*premium, close_val*premium, close_val*premium, 0.0
                    )
                )

        print(f"Successfully ingested {len(data_df)} rows for {label}")

    conn.commit()
    cur.close()
    conn.close()
    print("Ingestion complete.")

if __name__ == "__main__":
    ingest_vix()
