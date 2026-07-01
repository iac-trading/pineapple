import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import Optional, List, Dict, Any, Type

# Add the current directory to path to import adapters
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from adapters.base_adapter import BaseAdapter
from adapters.yfinance_adapter import YFinanceAdapter
from adapters.binance_adapter import BinanceAdapter
from adapters.macro_adapter import MacroAdapter

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("MarketDataManager")

class MarketDataManager:
    def __init__(self):
        self.db_params = {
            "host": os.getenv("POSTGRES_HOST", "192.168.100.201"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "dbname": os.getenv("POSTGRES_DB", "trading"),
            "user": os.getenv("POSTGRES_USER", "tsdb"),
            "password": os.environ["POSTGRES_PASSWORD"],
        }
        self.adapters: Dict[str, BaseAdapter] = {
            "yfinance": YFinanceAdapter(),
            "binance": BinanceAdapter(),
            "fred": MacroAdapter(),
        }

    def get_connection(self):
        return psycopg2.connect(**self.db_params)

    def register_adapter(self, name: str, adapter: BaseAdapter):
        self.adapters[name] = adapter
        logger.info(f"Registered adapter: {name}")

    def load_data(self, symbol: str, source: str, period: str = "5y", interval: str = "1h", api_key: Optional[str] = None):
        adapter = self.adapters.get(source)
        if not adapter:
            logger.error(f"Adapter not found: {source}")
            return

        # If it's the macro adapter and we have a key, we might need to re-init or set it
        if source == "fred" and api_key:
            adapter.api_key = api_key

        df = adapter.fetch_data(symbol, period, interval)
        if df is not None:
            records = adapter.parse_records(df, symbol, interval)
            table = "market_candles" if interval in ["1m", "5m", "15m", "30m", "1h", "1d"] else "market_ticks"
            self._bulk_insert(records, table)

    def _bulk_insert(self, records: List[tuple], table: str):
        if not records:
            logger.warning("No records to insert.")
            return
            
        if table == "market_candles":
            query = """
                INSERT INTO market_candles (ts, broker, symbol, granularity, open, high, low, close, volume, meta)
                VALUES %s
                ON CONFLICT (ts, broker, symbol, granularity) DO NOTHING
            """
        else:
            query = """
                INSERT INTO market_ticks (ts, broker, symbol, bid, ask, last, meta)
                VALUES %s
                ON CONFLICT (ts, broker, symbol) DO NOTHING
            """
        
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                execute_values(cur, query, records)
            conn.commit()
            logger.info(f"Successfully inserted {len(records)} records into {table}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to perform bulk insert into {table}: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="MarketDataManager V3 - Bulk Ingestion")
    parser.add_argument("--symbol", type=str, required=True, help="Trading symbol")
    parser.add_argument("--source", type=str, choices=["yfinance", "binance", "fred"], default="yfinance")
    parser.add_argument("--period", type=str, default="5y", help="Data period")
    parser.add_argument("--interval", type=str, default="1h", help="Data interval")
    parser.add_argument("--api-key", type=str, help="API Key for the source (e.g. FRED_API_KEY)")
    
    args = parser.parse_args()
    
    manager = MarketDataManager()
    manager.load_data(args.symbol, args.source, args.period, args.interval, api_key=args.api_key)
