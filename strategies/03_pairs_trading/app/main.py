import os
import sys
import asyncio
import json
import numpy as np
import pandas as pd
import statsmodels.api as sm
from typing import Optional, Dict, Any
from datetime import datetime
from scipy import stats

# Asegurar que las clases base estén en el path
sys.path.append(os.path.join(os.path.dirname(__file__), "../../common"))
from models import Tick, OrderSubmit
from strategy_base import StrategyBase
from runner_v3 import GenericRunnerV3

class KalmanPairsStrategy(StrategyBase):
    def __init__(self, instance_id: str, params: Dict[str, Any]):
        super().__init__(instance_id, params)
        self.symbol_a = params.get("symbol_a")
        self.symbol_b = params.get("symbol_b")
        self.z_entry = float(params.get("z_entry", 2.0))
        self.z_exit = float(params.get("z_exit", 0.0))
        self.lookback = int(params.get("lookback", 500))
        
        self.position = 0 # 0: None, 1: Long Spread (Buy A, Sell B), -1: Short Spread (Sell A, Buy B)
        
        # Kalman Filter States: [alpha, beta]
        self.state_mean = np.zeros(2)
        self.state_cov = np.eye(2)
        self.delta = 1e-5 # Process noise
        self.R = 0.01 # Measurement noise
        
        self.last_hedge_ratio = None
        self.structural_break_prob = 0.0

    async def on_tick(self, tick: Tick) -> Optional[OrderSubmit]:
        # 1. Almacenar el tick en el historial
        self._add_to_history(tick)
        
        # 2. Verificar si tenemos datos de ambos símbolos
        if self.symbol_a not in self.history or self.symbol_b not in self.history:
            return None
            
        hist_a = self.history[self.symbol_a]
        hist_b = self.history[self.symbol_b]
        
        if not hist_a or not hist_b:
            return None
            
        # Tomar los últimos precios
        price_a = hist_a[-1].last
        price_b = hist_b[-1].last
        
        # 3. Kalman Filter Update (Recalcular Hedge Ratio en cada tick)
        # Observation vector H = [1, price_b]
        H = np.array([1, price_b])
        
        # Prediction step
        self.state_cov += np.eye(2) * self.delta
        
        # Update step (Measurement)
        y = price_a - np.dot(H, self.state_mean) # Innovation / Residual
        S = np.dot(H, np.dot(self.state_cov, H.T)) + self.R # Innovation covariance
        K = np.dot(self.state_cov, H.T) / S # Kalman Gain
        
        self.state_mean += K * y
        self.state_cov -= np.outer(K, np.dot(H, self.state_cov))
        
        alpha, beta = self.state_mean
        self.last_hedge_ratio = beta
        
        # El spread actual es el residuo 'y' o (price_a - (alpha + beta * price_b))
        spread = price_a - (alpha + beta * price_b)
        
        # 4. Cópulas para Riesgo de Cola (Structural Break Detection)
        if len(hist_a) > 50 and len(hist_b) > 50:
            prices_a = np.array([t.last for t in hist_a[-50:]])
            prices_b = np.array([t.last for t in hist_b[-50:]])
            
            # Convertir a marginales uniformes (Empirical CDF)
            u = stats.rankdata(prices_a) / (len(prices_a) + 1)
            v = stats.rankdata(prices_b) / (len(prices_b) + 1)
            
            # Calcular dependencia de cola (Upper Tail Dependence Simplificado)
            # Si el mercado se está moviendo de forma extrema y la relación se rompe
            tail_threshold = 0.9
            upper_tail = np.sum((u > tail_threshold) & (v > tail_threshold)) / np.sum(u > tail_threshold)
            
            # Probabilidad de ruptura: a menor dependencia de cola detectada en momentos extremos, mayor riesgo
            # O simplemente si la correlación local cae drásticamente
            rho, _ = stats.spearmanr(prices_a, prices_b)
            self.structural_break_prob = 1.0 - abs(rho)
            
        # 5. Publicar a NATS
        await self.publish_analytics()

        # 6. Lógica de Ejecución (Z-Score dinámico)
        # Usamos una ventana móvil para el STD del spread (ruido)
        # ... (implementación simplificada de ejecución)
        
        z_score = spread / (np.sqrt(S) + 1e-9)
        
        order = None
        if self.structural_break_prob > 0.8:
            self.logger.warning(f"⚠️ Ruptura Estructural detectada ({self.structural_break_prob:.2f}). Evitando operar.")
            return None

        if self.position == 0:
            if z_score > self.z_entry:
                self.logger.info(f"🚨 Z-Score={z_score:.2f}. Hedge Ratio={beta:.4f}. Abriendo Short Spread.")
                self.position = -1
                order = self.create_order(self.symbol_a, "sell", self.params.get("qty", 1.0), 
                                        {"pair_sync": self.symbol_b, "side_b": "buy", "hedge_ratio": beta})
            elif z_score < -self.z_entry:
                self.logger.info(f"🚨 Z-Score={z_score:.2f}. Hedge Ratio={beta:.4f}. Abriendo Long Spread.")
                self.position = 1
                order = self.create_order(self.symbol_a, "buy", self.params.get("qty", 1.0), 
                                        {"pair_sync": self.symbol_b, "side_b": "sell", "hedge_ratio": beta})
        
        elif self.position != 0:
            if (self.position == -1 and z_score <= self.z_exit) or (self.position == 1 and z_score >= -self.z_exit):
                self.logger.info(f"✅ Reversión detectada (Z={z_score:.2f}). Cerrando posición.")
                self.position = 0
                order = self.create_order(self.symbol_a, "close", self.params.get("qty", 1.0))

        return order

    async def publish_analytics(self):
        payload = {
            "instance_id": self.instance_id,
            "symbol_a": self.symbol_a,
            "symbol_b": self.symbol_b,
            "hedge_ratio_dinamico": float(self.last_hedge_ratio),
            "probabilidad_ruptura_estructural": float(self.structural_break_prob),
            "timestamp": datetime.now().isoformat()
        }
        await self.runner.nc.publish("analytics.pairs_trading", json.dumps(payload).encode())

if __name__ == "__main__":
    runner = GenericRunnerV3(KalmanPairsStrategy)
    asyncio.run(runner.run())
