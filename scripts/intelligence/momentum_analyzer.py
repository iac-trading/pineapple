import os
import sys
import json
import pandas as pd
import psycopg2
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("MomentumAnalyzer")

class MomentumAnalyzer:
    def __init__(self):
        self.db_params = {
            "host": os.getenv("POSTGRES_HOST", "192.168.100.201"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "dbname": os.getenv("POSTGRES_DB", "trading"),
            "user": os.getenv("POSTGRES_USER", "tsdb"),
            "password": os.environ["POSTGRES_PASSWORD"],
        }

    def get_connection(self):
        return psycopg2.connect(**self.db_params)

    def fetch_universe_data(self, window_days=180):
        """Fetch daily close prices for the last window_days for all symbols."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=window_days + 30) # Extra buffer for weekends/holidays
        
        query = """
            SELECT ts, symbol, close
            FROM market_candles
            WHERE ts >= %s AND granularity = 86400
            ORDER BY symbol, ts ASC
        """
        
        conn = self.get_connection()
        try:
            df = pd.read_sql(query, conn, params=(start_date,))
            return df
        finally:
            conn.close()

    def run_ranking(self, top_n=50):
        """Calculate momentum and rank the universe."""
        df = self.fetch_universe_data()
        if df.empty:
            logger.warning("No data found in market_candles for momentum analysis.")
            return []

        # Convert to pivot table: index=ts, columns=symbol
        pivot_df = df.pivot(index='ts', columns='symbol', values='close')
        
        # Calculate 6-month returns (approx 126 trading days)
        # return = (Price_now / Price_6m_ago) - 1
        # We take the last available price and the price 126 rows back
        # If we have less than 126 rows, we skip that symbol or use max available
        window = 126
        returns = (pivot_df.iloc[-1] / pivot_df.iloc[-window]) - 1
        
        # Rank and filter Top N
        ranking = returns.dropna().sort_values(ascending=False)
        top_longs = ranking.head(top_n).index.tolist()
        top_shorts = ranking.tail(top_n).index.tolist()
        
        logger.info(f"Analysis Complete. Universe Size: {len(ranking)}")
        logger.info(f"Top Longs: {top_longs[:5]}")
        logger.info(f"Top Shorts: {top_shorts[:5]}")
        
        self.save_signals(top_longs, top_shorts)
        return top_longs, top_shorts

    def save_signals(self, longs, shorts):
        """Save rebalancing signals to the database/filesystem."""
        signal = {
            "ts": datetime.now().isoformat(),
            "strategy": "BP-51",
            "longs": longs,
            "shorts": shorts
        }
        # For now, we print and potentially write to a local signals dir
        output_path = "/home/ansible/platform/signals/momentum_daily.json"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(signal, f, indent=4)
        logger.info(f"Signals saved to {output_path}")

if __name__ == "__main__":
    analyzer = MomentumAnalyzer()
    analyzer.run_ranking()
