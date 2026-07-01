import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TAIL-ALPHA] %(message)s")
logger = logging.getLogger("TailRiskHedger")

class TailRiskHedger:
    """
    Estrategia 20: Tail Risk Hedging (Black Swan Protection)
    Compra Puts Out-of-the-Money (OTM) profundos sistemáticamente.
    Gasta un presupuesto fijo (1-2% anual) para proteger ante colapsos >10-20%.
    """
    def __init__(self, initial_capital=1000000.0):
        self.db_params = {
            "host": os.getenv("POSTGRES_HOST", "192.168.100.201"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "dbname": os.getenv("POSTGRES_DB", "trading"),
            "user": os.getenv("POSTGRES_USER", "tsdb"),
            "password": os.environ["POSTGRES_PASSWORD"]
        }
        self.initial_capital = initial_capital
        self.annual_budget_pct = 0.015 # 1.5% del portafolio al año
        self.weekly_budget = (self.initial_capital * self.annual_budget_pct) / 52

    def _get_conn(self):
        return psycopg2.connect(**self.db_params)

    def fetch_market_price(self, symbol="SPY"):
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT close FROM market_candles WHERE symbol=%s ORDER BY ts DESC LIMIT 1",
                    (symbol,)
                )
                res = cur.fetchone()
                return float(res[0]) if res else None

    def calculate_hedge(self, spot_price):
        """
        Calcula qué Puts comprar basados en el presupuesto semanal.
        Objetivo: Puts a 30-45 días con strike 15% OTM.
        """
        if not spot_price:
            return None
            
        target_strike = spot_price * 0.85 # 15% por debajo del precio actual
        
        # En una versión real, consultaríamos la cadena de opciones (ThetaData/IBKR)
        # Aquí generamos la instrucción táctica para el broker.
        exp_date = (datetime.now() + timedelta(days=35)).strftime("%Y-%m-%d")
        
        result = {
            "ts": datetime.now().isoformat(),
            "underlying": "SPY",
            "spot": spot_price,
            "target_strike": round(target_strike, 2),
            "expiration": exp_date,
            "weekly_budget_usd": round(self.weekly_budget, 2),
            "estimated_contracts": max(1, int(self.weekly_budget / 50)) # Simulando costo de $0.50 ($50 por contrato)
        }
        
        logger.info(f"Tail Hedge Signal: Buy {result['estimated_contracts']} Contracts | Strike: {result['target_strike']} | Exp: {exp_date}")
        return result

if __name__ == "__main__":
    hedger = TailRiskHedger()
    spot = hedger.fetch_market_price("SPY")
    print(hedger.calculate_hedge(spot))
