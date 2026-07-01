import yfinance as yf
import pandas as pd
import logging
from typing import Optional, List
from .base_adapter import BaseAdapter

logger = logging.getLogger("YFinanceAdapter")

class YFinanceAdapter(BaseAdapter):
    def fetch_data(self, symbol: str, period: str, interval: str) -> Optional[pd.DataFrame]:
        # yfinance limits: 1h data is only available for the last 730 days.
        if interval == "1h" and period.endswith("y"):
            num_years = int(period.replace("y", ""))
            if num_years > 2:
                logger.warning(f"Interval 1h only supports 730 days. Clipping {period} to 729d.")
                period = "729d"
        
        logger.info(f"Downloading {symbol} from yfinance (period={period}, interval={interval})...")
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period, interval=interval)
        
        if df.empty:
            logger.warning(f"No data found for {symbol}")
            return None
        return df

    def parse_records(self, df: pd.DataFrame, symbol: str, interval: str) -> List[tuple]:
        is_candle = interval in ["1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo"]
        records = []
        broker = "yfinance"
        granularity = self.get_granularity(interval)

        for ts, row in df.iterrows():
            if is_candle:
                # ts, broker, symbol, granularity, open, high, low, close, volume, meta
                records.append((
                    ts.to_pydatetime(),
                    broker,
                    symbol,
                    granularity,
                    float(row['Open']),
                    float(row['High']),
                    float(row['Low']),
                    float(row['Close']),
                    float(row['Volume']),
                    '{"source": "yfinance"}'
                ))
            else:
                # ts, broker, symbol, bid, ask, last, meta
                records.append((
                    ts.to_pydatetime(),
                    broker,
                    symbol,
                    float(row['Close']), # bid
                    float(row['Close']), # ask
                    float(row['Close']), # last
                    f'{{"source": "yfinance", "volume": {float(row["Volume"])}}}'
                ))
        return records
