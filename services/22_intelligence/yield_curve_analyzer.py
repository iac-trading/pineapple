import os
import logging
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [YIELD-ALPHA] %(message)s")
logger = logging.getLogger("YieldCurveButterfly")

class YieldCurveButterflyAnalyzer:
    """
    Estrategia 12: Yield Curve Butterfly Arbitrage
    Opera la curvatura (convexidad) de la curva de rendimientos (2Y, 5Y, 10Y).
    Butterfly = (2Y - 5Y) - (5Y - 10Y) = 2Y + 10Y - 2*5Y
    Si el Butterfly está en niveles extremos (desviación respecto a media móvil), 
    se apuesta a la reversión de la curvatura.
    """
    def __init__(self):
        self.db_params = {
            "host": os.getenv("POSTGRES_HOST", "192.168.100.201"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "dbname": os.getenv("POSTGRES_DB", "trading"),
            "user": os.getenv("POSTGRES_USER", "tsdb"),
            "password": os.environ["POSTGRES_PASSWORD"]
        }

    def _get_conn(self):
        return psycopg2.connect(**self.db_params)

    def fetch_yields(self, days=120):
        """
        Obtiene precios de futuros ZT (2Y), ZF (5Y), ZN (10Y).
        En Yahoo Finance: ZT=F, ZF=F, ZN=F.
        """
        end_ts = datetime.now()
        start_ts = end_ts - timedelta(days=days)
        
        symbols = ["ZT=F", "ZF=F", "ZN=F"]
        data = {}
        
        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for sym in symbols:
                    cur.execute(
                        "SELECT ts, close FROM market_candles WHERE symbol=%s AND ts >= %s ORDER BY ts ASC",
                        (sym, start_ts)
                    )
                    rows = cur.fetchall()
                    if rows:
                        data[sym] = pd.DataFrame(rows).set_index("ts")["close"]
        
        return pd.DataFrame(data).ffill()

    def analyze_curvature(self):
        """
        Calcula el valor del Butterfly y genera señales.
        """
        df = self.fetch_yields()
        if df.empty or len(df.columns) < 3:
            logger.warning("Missing Yield Curve data (ZT=F, ZF=F, ZN=F)")
            return None
            
        # Butterfly = 2Y + 10Y - 2*5Y (en precios)
        df["butterfly"] = df["ZT=F"] + df["ZN=F"] - 2 * df["ZF=F"]
        
        # Calcular Z-Score de la curvatura
        window = 20
        df["mean"] = df["butterfly"].rolling(window).mean()
        df["std"] = df["butterfly"].rolling(window).std()
        df["zscore"] = (df["butterfly"] - df["mean"]) / df["std"]
        
        latest = df.iloc[-1]
        z = latest["zscore"]
        
        action = "WAIT"
        signal = 0
        
        if z > 2.0:
            action = "SELL_BUTTERFLY" # Short 2Y/10Y, Long 5Y
            signal = -1
        elif z < -2.0:
            action = "BUY_BUTTERFLY"  # Long 2Y/10Y, Short 5Y
            signal = 1
            
        result = {
            "ts": latest.name.isoformat(),
            "butterfly_val": float(latest["butterfly"]),
            "zscore": round(float(z), 2),
            "signal": signal,
            "action": action,
            "zt": float(latest["ZT=F"]),
            "zf": float(latest["ZF=F"]),
            "zn": float(latest["ZN=F"])
        }
        
        logger.info(f"Yield Signal: {action} (Z: {result['zscore']})")
        return result

if __name__ == "__main__":
    analyzer = YieldCurveButterflyAnalyzer()
    print(analyzer.analyze_curvature())
