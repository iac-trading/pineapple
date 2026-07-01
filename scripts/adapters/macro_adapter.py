import requests
import pandas as pd
import logging
import os
from typing import Optional, List
from .base_adapter import BaseAdapter

logger = logging.getLogger("MacroAdapter")

class MacroAdapter(BaseAdapter):
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("FRED_API_KEY")

    def fetch_data(self, symbol: str, period: str, interval: str) -> Optional[pd.DataFrame]:
        if not self.api_key:
            logger.warning("FRED_API_KEY not found. Data fetch will likely fail.")
            return None

        # symbol here is the FRED series ID (e.g., FEDFUNDS, CPIAUCSL)
        params = {
            "series_id": symbol,
            "api_key": self.api_key,
            "file_type": "json"
        }

        logger.info(f"Downloading {symbol} from FRED...")
        try:
            response = requests.get(self.BASE_URL, params=params)
            if response.status_code != 200:
                logger.error(f"FRED API Error {response.status_code}: {response.text}")
                return None
            data = response.json()
        except Exception as e:
            logger.error(f"FRED fetch failed: {e}")
            return None

        if not data or 'observations' not in data:
            logger.warning(f"No data returned for {symbol}")
            return None

        df = pd.DataFrame(data['observations'])
        df['timestamp'] = pd.to_datetime(df['date'])
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df.dropna(subset=['value'], inplace=True)
        df.set_index('timestamp', inplace=True)
        return df

    def parse_records(self, df: pd.DataFrame, symbol: str, interval: str) -> List[tuple]:
        records = []
        broker = "fred"
        granularity = 86400 # Default to daily for macro

        for ts, row in df.iterrows():
            records.append((
                ts.to_pydatetime(),
                broker,
                symbol,
                granularity,
                float(row['value']), # open
                float(row['value']), # high
                float(row['value']), # low
                float(row['value']), # close
                0.0, # volume
                f'{{"source": "fred", "series": "{symbol}"}}'
            ))
        return records
