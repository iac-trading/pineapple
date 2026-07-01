import os
import logging
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [VIX-ALPHA] %(message)s")
logger = logging.getLogger("VixTermStructure")

class VixTermStructureAnalyzer:
    """
    Estrategia 11: VIX Term Structure Alpha
    Analiza el spread entre el primer (F1) y segundo (F2) mes de futuros del VIX.
    Contango (F2 > F1): Mercado calmo, se vende volatilidad.
    Backwardation (F1 > F2): Mercado estresado, se cierra venta o se rota a cash.
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

    def fetch_vix_futures(self, days=30):
        """
        Obtiene los precios de cierre recientes de los futuros VIX.
        Asumimos que en market_candles tenemos VX=F (F1) y VX_next (F2).
        En Yahoo Finance, VX=F es el front month.
        """
        end_ts = datetime.now()
        start_ts = end_ts - timedelta(days=days)
        
        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Obtenemos F1 (Front Month)
                cur.execute(
                    "SELECT ts, close FROM market_candles WHERE symbol='VX=F' AND ts >= %s ORDER BY ts ASC",
                    (start_ts,)
                )
                f1_data = cur.fetchall()
                
                # Obtenemos F2 (Next Month - En YFinance a veces es VX=F con otra expiración o un ticker sintético)
                # Para este diseño, simulamos F2 como ^VIX * 1.05 si no hay data, 
                # o buscamos un ticker secundario.
                # USANDO VALORES REALES SI DISPONIBLES:
                cur.execute(
                    "SELECT ts, close FROM market_candles WHERE symbol='VX=F_NEXT' AND ts >= %s ORDER BY ts ASC",
                    (start_ts,)
                )
                f2_data = cur.fetchall()
                
        return pd.DataFrame(f1_data), pd.DataFrame(f2_data)

    def calculate_roll_yield(self, f1_df, f2_df):
        if f1_df.empty or f2_df.empty:
            logger.warning("Missing VIX futures data for roll yield calculation")
            return None
            
        df = pd.merge(f1_df, f2_df, on="ts", suffixes=("_f1", "_f2")).set_index("ts")
        df["contango"] = (df["close_f2"] - df["close_f1"]) / df["close_f1"]
        return df

    def generate_signal(self):
        """
        Genera la señal de trading para la Estrategia 11.
        Contango > 5% -> STRONG_SELL_VOL (Long VXX / Short UVXY)
        Contango < 0 (Backwardation) -> EXIT_POS
        """
        f1, f2 = self.fetch_vix_futures()
        df = self.calculate_roll_yield(f1, f2)
        
        if df is None or df.empty:
            return {"status": "NO_DATA", "signal": 0}
            
        latest = df.iloc[-1]
        contango_pct = latest["contango"]
        
        signal = 0
        action = "WAIT"
        
        if contango_pct > 0.05:
            signal = -1  # Sell Volatility
            action = "SELL_VOL"
        elif contango_pct < 0:
            signal = 0   # Exit
            action = "EXIT_FLAT"
            
        result = {
            "ts": latest.name.isoformat(),
            "f1": float(latest["close_f1"]),
            "f2": float(latest["close_f2"]),
            "contango_pct": round(float(contango_pct) * 100, 2),
            "signal": signal,
            "action": action
        }
        
        logger.info(f"VIX Signal: {action} (Contango: {result['contango_pct']}%)")
        return result

if __name__ == "__main__":
    analyzer = VixTermStructureAnalyzer()
    print(analyzer.generate_signal())
