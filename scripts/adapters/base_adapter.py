from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional, List, Dict, Any

class BaseAdapter(ABC):
    @abstractmethod
    def fetch_data(self, symbol: str, period: str, interval: str) -> Optional[pd.DataFrame]:
        """Fetch raw data from the source."""
        pass

    @abstractmethod
    def parse_records(self, df: pd.DataFrame, symbol: str, interval: str) -> List[tuple]:
        """Parse DataFrame into database-ready records."""
        pass

    def get_granularity(self, interval: str) -> int:
        """Map interval string to seconds."""
        gran_map = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "1d": 86400,
        }
        return gran_map.get(interval, 3600)
