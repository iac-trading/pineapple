import numpy as np
import pandas as pd
from typing import Union

def calculate_hurst(ts: Union[pd.Series, np.ndarray, list], max_lag: int = 20) -> float:
    """
    Calcula el Exponente de Hurst (H) usando el método de Rescaled Range (R/S).
    H < 0.5: Mean Reverting (Rango)
    H = 0.5: Random Walk (Paseo aleatorio)
    H > 0.5: Trending (Tendencia)
    """
    if len(ts) < max_lag * 2:
        return 0.5 # Default random walk if not enough data
        
    ts = np.array(ts)
    lags = range(2, max_lag)
    
    # R/S analysis components
    tau = [np.sqrt(np.std(np.subtract(ts[lag:], ts[:-lag]))) for lag in lags]
    
    # Regresión lineal para encontrar la pendiente de log(tau) vs log(lags)
    poly = np.polyfit(np.log(lags), np.log(tau), 1)
    
    return poly[0] * 2.0

class RegimeFilter:
    """Helper para aplicar filtros de régimen a estrategias"""
    @staticmethod
    def is_trending(prices: list, window: int = 100, threshold: float = 0.55) -> bool:
        if len(prices) < window:
            return False
        h = calculate_hurst(prices[-window:])
        return h > threshold

    @staticmethod
    def is_ranging(prices: list, window: int = 100, threshold: float = 0.45) -> bool:
        if len(prices) < window:
            return False
        h = calculate_hurst(prices[-window:])
        return h < threshold
