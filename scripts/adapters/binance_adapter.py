import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Optional, List
from .base_adapter import BaseAdapter

logger = logging.getLogger("BinanceAdapter")

class BinanceAdapter(BaseAdapter):
    BASE_URL = "https://fapi.binance.com/fapi/v1/klines"

    def fetch_data(self, symbol: str, period: str, interval: str) -> Optional[pd.DataFrame]:
        # Convert period to start_time
        now = datetime.utcnow()
        if period.endswith("d"):
            days = int(period.replace("d", ""))
            start_time = now - timedelta(days=days)
        elif period.endswith("y"):
            years = int(period.replace("y", ""))
            start_time = now - timedelta(days=years * 365)
        else:
            start_time = now - timedelta(days=30) # Default

        # Binance expects symbol in uppercase without dash or slash (e.g., BTCUSDT)
        clean_symbol = symbol.replace("-", "").replace("/", "").upper()
        
        # If the symbol doesn't already have a valid quote (USDT/BUSD), default to USDT
        if not any(clean_symbol.endswith(q) for q in ["USDT", "BUSD", "USDC"]):
            # If it already ends with USD, change to USDT
            if clean_symbol.endswith("USD"):
                binance_symbol = clean_symbol + "T"
            else:
                binance_symbol = clean_symbol + "USDT"
        else:
            binance_symbol = clean_symbol

        # Binance params
        params = {
            "symbol": binance_symbol,
            "interval": interval,
            "startTime": int(start_time.timestamp() * 1000),
            "limit": 1500 # Binance max limit per page
        }

        logger.info(f"Downloading {binance_symbol} from Binance (period={period}, interval={interval})...")
        try:
            response = requests.get(self.BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.error(f"Binance fetch failed: {e}")
            return None

        if not data:
            logger.warning(f"No data returned for {binance_symbol}")
            return None

        # [Open time, Open, High, Low, Close, Volume, Close time, Quote asset volume, Number of trades, Taker buy base asset volume, Taker buy quote asset volume, Ignore]
        df = pd.DataFrame(data, columns=[
            'timestamp', 'open', 'high', 'low', 'close', 'volume', 
            'close_time', 'quote_asset_volume', 'number_of_trades', 
            'taker_buy_base', 'taker_buy_quote', 'ignore'
        ])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        return df

    def parse_records(self, df: pd.DataFrame, symbol: str, interval: str) -> List[tuple]:
        records = []
        broker = "binance"
        granularity = self.get_granularity(interval)

        for ts, row in df.iterrows():
            # ts, broker, symbol, granularity, open, high, low, close, volume, meta
            records.append((
                ts.to_pydatetime(),
                broker,
                symbol,
                granularity,
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close']),
                float(row['volume']),
                f'{{"source": "binance", "trades": {row["number_of_trades"]}}}'
            ))
        return records
