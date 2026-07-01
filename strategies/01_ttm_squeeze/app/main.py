import os
import sys
import json
import logging
import asyncio
import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, List

# Asegurar que las clases base estén en el path
sys.path.append(os.path.join(os.path.dirname(__file__), "../../common"))
from models import Tick
from strategy_base import StrategyBase
from runner_v3 import GenericRunnerV3

class TTMSqueezeStrategy(StrategyBase):
    """
    Estrategia 01: TTM Squeeze.
    Detecta consolidación (Squeeze ON) y opera la ruptura con Momentum.
    """
    def __init__(self, instance_id: str, params: Dict[str, Any]):
        super().__init__(instance_id, params)
        self.period = int(params.get("period", 20))
        self.bb_mult = float(params.get("bb_mult", 2.0))
        self.kc_mult = float(params.get("kc_mult", 1.5))
        self.prices = []
        self.squeeze_on = False

    async def on_tick(self, tick: Tick) -> Optional[Any]:
        px = tick.last if tick.last else ((tick.bid + tick.ask)/2.0 if tick.bid and tick.ask else 0.0)
        if px == 0: return None
        
        self.prices.append(px)
        if len(self.prices) > self.period + 5:
            self.prices.pop(0)
            
        if len(self.prices) < self.period:
            return None

        # 1. BB & KC (Aproximación eficiente para live)
        prices_sr = pd.Series(self.prices)
        ma = prices_sr.mean()
        std = prices_sr.std()
        
        # TR simplificado para KC
        tr = prices_sr.diff().abs().mean()
        
        bb_upper = ma + (self.bb_mult * std)
        bb_lower = ma - (self.bb_mult * std)
        kc_upper = ma + (self.kc_mult * tr)
        kc_lower = ma - (self.kc_mult * tr)
        
        prev_squeeze = self.squeeze_on
        self.squeeze_on = (bb_upper < kc_upper) and (bb_lower > kc_lower)
        
        # 2. Momentum (Diferencia vs promedio de extremos)
        highest = max(self.prices)
        lowest = min(self.prices)
        avg = (highest + lowest + ma) / 3.0
        mom_val = px - avg
        
        # 3. Execution (Squeeze Fired)
        fired = prev_squeeze and not self.squeeze_on
        
        if fired:
            if mom_val > 0:
                self.logger.info(f"TTM Squeeze FIRED UP on {tick.symbol} | Mom: {mom_val:.4f}")
                return self.create_order(symbol=tick.symbol, side="buy", qty=self.params.get("qty", 1.0))
            elif mom_val < 0:
                self.logger.info(f"TTM Squeeze FIRED DOWN on {tick.symbol} | Mom: {mom_val:.4f}")
                return self.create_order(symbol=tick.symbol, side="sell", qty=self.params.get("qty", 1.0))

        return None

if __name__ == "__main__":
    runner = GenericRunnerV3(TTMSqueezeStrategy)
    asyncio.run(runner.run())
