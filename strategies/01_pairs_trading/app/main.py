"""
=============================================================================
STRATEGY 1: PAIRS TRADING (STATARB)
=============================================================================
Lógica: Sincroniza dos activos (A y B), calcula el hedge ratio mediante OLS,
y opera la reversión a la media del Z-Score del spread.
=============================================================================
"""

import numpy as np
import statsmodels.api as sm
from collections import deque
from typing import Optional, Dict, Any, List
from strategies.common.strategy_base import StrategyBase
from strategies.common.models import Tick, OrderSubmit

class PairsTradingStrategy(StrategyBase):
    def __init__(self, instance_id: str, params: Dict[str, Any]):
        super().__init__(instance_id, params)
        self.symbol_a = params.get("symbol_a")
        self.symbol_b = params.get("symbol_b")
        self.z_entry = float(params.get("z_entry", 2.0))
        self.z_exit = float(params.get("z_exit", 0.5))
        
        # Buffer sincronizado para par
        self.lookback = int(params.get("lookback", 100))
        self.buffer_a = deque(maxlen=self.lookback)
        self.buffer_b = deque(maxlen=self.lookback)
        
        self.position = 0 # 1: Long Spread, -1: Short Spread, 0: None

    def on_tick(self, tick: Tick) -> Optional[List[OrderSubmit]]:
        # 1. Sincronización
        if tick.symbol == self.symbol_a:
            self.buffer_a.append(tick.last)
        elif tick.symbol == self.symbol_b:
            self.buffer_b.append(tick.last)
        
        # Esperar a tener buffers llenos y sincronizados
        if len(self.buffer_a) < self.lookback or len(self.buffer_b) < self.lookback:
            return None

        # 2. Cálculo de Spread y Z-Score (OLS)
        # y = beta * x + alpha
        y = np.array(self.buffer_a)
        x = sm.add_constant(np.array(self.buffer_b))
        
        model = sm.OLS(y, x).fit()
        beta = model.params[1]
        
        spread = np.array(self.buffer_a) - (beta * np.array(self.buffer_b))
        z_score = (spread[-1] - np.mean(spread)) / np.std(spread)
        
        qty = self.params.get("qty", 1.0)
        orders = []

        # 3. Lógica de Trading
        if self.position == 0:
            if z_score > self.z_entry:
                # Short Spread: Sell A, Buy B
                self.position = -1
                orders.append(self.create_order(self.symbol_a, 'sell', qty))
                orders.append(self.create_order(self.symbol_b, 'buy', qty * beta))
                self.logger.info(f"Short Spread | Z-Score: {z_score:.2f} | Hedge: {beta:.2f}")
                
            elif z_score < -self.z_entry:
                # Long Spread: Buy A, Sell B
                self.position = 1
                orders.append(self.create_order(self.symbol_a, 'buy', qty))
                orders.append(self.create_order(self.symbol_b, 'sell', qty * beta))
                self.logger.info(f"Long Spread | Z-Score: {z_score:.2f} | Hedge: {beta:.2f}")

        elif self.position == 1 and z_score > -self.z_exit:
            # Salida Long Spread
            self.position = 0
            orders.append(self.create_order(self.symbol_a, 'sell', qty))
            orders.append(self.create_order(self.symbol_b, 'buy', qty * beta))
            self.logger.info(f"Exit Long Spread | Z-Score: {z_score:.2f}")

        elif self.position == -1 and z_score < self.z_exit:
            # Salida Short Spread
            self.position = 0
            orders.append(self.create_order(self.symbol_a, 'buy', qty))
            orders.append(self.create_order(self.symbol_b, 'sell', qty * beta))
            self.logger.info(f"Exit Short Spread | Z-Score: {z_score:.2f}")

        return orders if orders else None
