import os
import ccxt
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values

DB_USER = os.getenv("POSTGRES_USER", "tsdb")
DB_PASS = os.environ["POSTGRES_PASSWORD"]
DB_HOST = os.getenv("POSTGRES_HOST", "192.168.100.201")
DB_NAME = os.getenv("POSTGRES_DB", "trading")

DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

def get_conn():
    return psycopg2.connect(DB_URL)

def ingest_spot(symbol="BTC/USDT"):
    print(f"Ingesting Spot 1h for {symbol}...")
    ex = ccxt.binance()
    since = ex.parse8601('2025-01-01T00:00:00Z')
    all_data = []
    
    while since < ex.milliseconds():
        print(f"Fetching {symbol} from {ex.iso8601(since)}")
        ohlcv = ex.fetch_ohlcv(symbol, '1h', since=since, limit=1000)
        if not ohlcv: break
        all_data.extend(ohlcv)
        since = ohlcv[-1][0] + 1
        
    df = pd.DataFrame(all_data, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
    df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
    df.drop_duplicates('ts', inplace=True)
    
    db_sym = symbol.replace("/", "")
    records = []
    for _, r in df.iterrows():
        records.append((
            r['ts'].to_pydatetime(), 'binance', db_sym, 3600, 
            float(r['open']), float(r['high']), float(r['low']), float(r['close']), float(r['vol'])
        ))
        
    conn = get_conn()
    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO market_candles (ts, broker, symbol, granularity, open, high, low, close, volume)
        VALUES %s ON CONFLICT DO NOTHING
    """, records)
    conn.commit()
    print(f"Saved {len(records)} Spot candles for {db_sym}")

def ingest_perp(symbol="BTC/USDT:USDT"):
    print(f"Ingesting Perp 1h for {symbol}...")
    ex = ccxt.binance({'options': {'defaultType': 'future'}})
    since = ex.parse8601('2025-01-01T00:00:00Z')
    all_data = []
    
    while since < ex.milliseconds():
        print(f"Fetching {symbol} from {ex.iso8601(since)}")
        ohlcv = ex.fetch_ohlcv(symbol, '1h', since=since, limit=1000)
        if not ohlcv: break
        all_data.extend(ohlcv)
        since = ohlcv[-1][0] + 1
        
    df = pd.DataFrame(all_data, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
    df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
    df.drop_duplicates('ts', inplace=True)
    
    # Base spot is BTCUSDT. Perp is BTCUSDT-PERP
    db_sym = symbol.split(":")[0].replace("/", "") + "-PERP"
    records = []
    for _, r in df.iterrows():
        records.append((
            r['ts'].to_pydatetime(), 'binance', db_sym, 3600, 
            float(r['open']), float(r['high']), float(r['low']), float(r['close']), float(r['vol'])
        ))
        
    conn = get_conn()
    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO market_candles (ts, broker, symbol, granularity, open, high, low, close, volume)
        VALUES %s ON CONFLICT DO NOTHING
    """, records)
    conn.commit()
    print(f"Saved {len(records)} Perp candles for {db_sym}")

def ingest_funding(symbol="BTC/USDT:USDT"):
    print(f"Ingesting Funding for {symbol}...")
    ex = ccxt.binance({'options': {'defaultType': 'future'}})
    since = ex.parse8601('2025-01-01T00:00:00Z')
    all_data = []
    limit = 1000
    
    for _ in range(10):
        print(f"Fetching funding {symbol} from {ex.iso8601(since)}")
        rates = ex.fetch_funding_rate_history(symbol, since=since, limit=limit)
        if not rates: break
        all_data.extend(rates)
        
        last_ts = rates[-1]['timestamp']
        if last_ts == since: break
        since = last_ts + 1
        if len(rates) < limit: break
            
    df = pd.DataFrame(all_data)
    df['ts'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    df.drop_duplicates('ts', inplace=True)
    
    records = []
    for _, r in df.iterrows():
        records.append((
            r['ts'].to_pydatetime(), 'binance', symbol, float(r['fundingRate'])
        ))
        
    conn = get_conn()
    cur = conn.cursor()
    execute_values(cur, """
        INSERT INTO market_funding (ts, broker, symbol, rate)
        VALUES %s ON CONFLICT DO NOTHING
    """, records)
    conn.commit()
    print(f"Saved {len(records)} Funding rates for {symbol}")

if __name__ == "__main__":
    for sym in ["BTC/USDT", "ETH/USDT"]:
        ingest_spot(sym)
        ingest_perp(f"{sym}:USDT")
        ingest_funding(f"{sym}:USDT")
    print("Data ingestion complete!")
