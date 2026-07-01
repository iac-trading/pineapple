import os
import logging
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PCA-MATRIX] %(message)s")
logger = logging.getLogger("PCAMatrixArb")

class PCAMatrixAnalyzer:
    """
    Estrategia 18: PCA-based Matrix Arbitrage.
    Analiza una matriz de retornos de multiples activos. 
    Explota las desviaciones (residuales) respecto a los componentes principales del mercado.
    """
    def __init__(self, symbols=["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA"]):
        self.symbols = symbols
        self.db_params = {
            "host": os.getenv("POSTGRES_HOST", "192.168.100.201"),
            "port": int(os.getenv("POSTGRES_PORT", "5432")),
            "dbname": os.getenv("POSTGRES_DB", "trading"),
            "user": os.getenv("POSTGRES_USER", "tsdb"),
            "password": os.environ["POSTGRES_PASSWORD"]
        }

    def _get_conn(self):
        return psycopg2.connect(**self.db_params)

    def fetch_matrix_data(self, days=180):
        end_ts = datetime.now()
        start_ts = end_ts - timedelta(days=days)
        
        matrix = {}
        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for symbol in self.symbols:
                    cur.execute(
                        "SELECT ts, close FROM market_candles WHERE symbol=%s AND ts >= %s ORDER BY ts ASC",
                        (symbol, start_ts)
                    )
                    rows = cur.fetchall()
                    if rows:
                        matrix[symbol] = pd.DataFrame(rows).set_index("ts")["close"]
        
        df = pd.DataFrame(matrix).ffill().pct_change().dropna()
        return df

    def calculate_signals(self, n_components=3):
        df_returns = self.fetch_matrix_data()
        if df_returns.empty or len(df_returns.columns) < n_components + 1:
            logger.warning("Insufficient data or symbols for PCA analysis")
            return []
            
        # Standardize returns
        scaler = StandardScaler()
        scaled_returns = scaler.fit_transform(df_returns)
        
        # Fit PCA
        pca = PCA(n_components=n_components)
        pca.fit(scaled_returns)
        
        # Reconstruct returns using components
        components = pca.transform(scaled_returns)
        reconstructed = pca.inverse_transform(components)
        
        # Residuals = Actual - Reconstructed
        residuals = scaled_returns - reconstructed
        df_residuals = pd.DataFrame(residuals, index=df_returns.index, columns=df_returns.columns)
        
        # Calculate Z-Score of residuals (last 20 days)
        last_residuals = df_residuals.tail(20)
        zscores = (df_residuals.iloc[-1] - last_residuals.mean()) / (last_residuals.std() + 1e-9)
        
        signals = []
        for symbol in self.symbols:
            z = zscores[symbol]
            action = "WAIT"
            val = 0
            
            if z > 2.0:
                action = "SHORT" # Sobre-extendido al alza vs mercado
                val = -1
            elif z < -2.0:
                action = "LONG" # Sobre-extendido a la baja vs mercado
                val = 1
                
            signals.append({
                "ts": df_returns.index[-1].isoformat(),
                "symbol": symbol,
                "zscore": float(z),
                "action": action,
                "signal": val
            })
            
        logger.info(f"PCA Analysis complete. Generated {len([s for s in signals if s['signal'] != 0])} trading signals.")
        return signals

if __name__ == "__main__":
    analyzer = PCAMatrixAnalyzer()
    print(analyzer.calculate_signals())
