import os
import logging
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [COMMODITY-ALPHA] %(message)s")
logger = logging.getLogger("CommodityRollYield")

class CommodityRollYieldAnalyzer:
    """
    Estrategia 16: Commodity Roll Yield
    Monitoriza la estructura temporal de commodities (ej: Crude Oil CL=F).
    Si el mercado está en Backwardation (F1 > F2), el roll yield es positivo para largos.
    Si está en Contango (F2 > F1), el roll yield es positivo para cortos.
    """
    def __init__(self, symbol="CL=F"):
        self.symbol = symbol
        self.db_params = {
            "host": os.getenv("POSTGRES_HOST", "192.168.100.201"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "dbname": os.getenv("POSTGRES_DB", "trading"),
            "user": os.getenv("POSTGRES_USER", "tsdb"),
            "password": os.environ["POSTGRES_PASSWORD"]
        }

    def _get_conn(self):
        return psycopg2.connect(**self.db_params)

    def fetch_data(self, days=90):
        # Necesitamos F1 y F2 (Simulado como F1_NEXT)
        end_ts = datetime.now()
        start_ts = end_ts - timedelta(days=days)
        
        data = {}
        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for target in [self.symbol, f"{self.symbol}_NEXT"]:
                    cur.execute(
                        "SELECT ts, close FROM market_candles WHERE symbol=%s AND ts >= %s ORDER BY ts ASC",
                        (target, start_ts)
                    )
                    rows = cur.fetchall()
                    if rows:
                        data[target] = pd.DataFrame(rows).set_index("ts")["close"]
        
        return pd.DataFrame(data).ffill()

    def calculate_signal(self):
        df = self.fetch_data()
        if df.empty or len(df.columns) < 2:
            logger.warning(f"Insufficient data for {self.symbol} roll yield")
            return None
            
        f1_col = self.symbol
        f2_col = f"{self.symbol}_NEXT"
        
        # Roll Yield Annualized = (F1 - F2) / F1 * (365 / days_to_expiry)
        # Simplificado: (F1/F2) - 1
        df["roll_yield"] = (df[f1_col] / df[f2_col]) - 1
        
        latest = df.iloc[-1]
        ry = latest["roll_yield"]
        
        action = "WAIT"
        signal = 0
        
        # Umbral del 1% de spread para entrar
        if ry > 0.01:
            action = "LONG_COMMODITY" # Backwardation -> Roll yield positivo para largos
            signal = 1
        elif ry < -0.01:
            action = "SHORT_COMMODITY" # Contango -> Roll yield positivo para cortos
            signal = -1
            
        result = {
            "ts": latest.name.isoformat(),
            "symbol": self.symbol,
            "roll_yield": float(ry),
            "signal": signal,
            "action": action,
            "f1": float(latest[f1_col]),
            "f2": float(latest[f2_col])
        }
        
        logger.info(f"Commodity Signal ({self.symbol}): {action} (RY: {result['roll_yield']:.4f})")
        return result

if __name__ == "__main__":
    analyzer = CommodityRollYieldAnalyzer()
    print(analyzer.calculate_signal())
