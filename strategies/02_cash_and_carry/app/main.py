import os
import sys
import json
import logging
import asyncio
from typing import Dict, Any, Optional, List

# Asegurar que las clases base estén en el path
sys.path.append(os.path.join(os.path.dirname(__file__), "../../common"))
from models import Tick, OrderSubmit
from strategy_base import StrategyBase
from runner_v3 import GenericRunnerV3
from util import safe_float, utc_now_iso

class CashAndCarryStrategy(StrategyBase):
    """
    Estrategia 02: Cash & Carry (Funding Arbitrage)
    """
    def __init__(self, instance_id: str, params: Dict[str, Any]):
        super().__init__(instance_id, params)
        self.base_symbol = params.get("base_symbol", "BTCUSDT")
        self.perp_symbol = params.get("perp_symbol", f"{self.base_symbol}-PERP")
        self.spot_price = 0.0
        self.perp_price = 0.0
        self.funding_rate = 0.0
        self.threshold = float(params.get("threshold", 0.0001))
        self.is_active = False

    async def on_tick(self, tick: Tick) -> Optional[List[OrderSubmit]]:
        self.logger.info(f"📥 Received tick for {tick.symbol}: {tick.last}")
        self._add_to_history(tick)
        if tick.symbol == self.base_symbol:
            self.spot_price = tick.last if tick.last else ((tick.bid + tick.ask)/2.0 if tick.bid and tick.ask else 0.0)
            self.logger.info(f"🔹 Updated Spot Price: {self.spot_price}")
        elif tick.symbol == self.perp_symbol:
            self.perp_price = tick.last if tick.last else ((tick.bid + tick.ask)/2.0 if tick.bid and tick.ask else 0.0)
            self.logger.info(f"🔹 Updated Perp Price: {self.perp_price}")
        elif tick.symbol.endswith(":FUNDING"):
            self.funding_rate = tick.last if tick.last else 0.0
            self.logger.info(f"🔹 Updated Funding Rate: {self.funding_rate}")
        return self._check_arbitrage()

    def _check_arbitrage(self) -> Optional[List[OrderSubmit]]:
        if self.spot_price == 0 or self.perp_price == 0:
            self.logger.info(f"⏳ Waiting for prices... Spot: {self.spot_price} | Perp: {self.perp_price}")
            return None
        
        spread = (self.perp_price / self.spot_price) - 1.0 if self.spot_price != 0 else 0.0
        
        # Misión 1: Disparar por funding O por spread (según parámetros)
        effective_threshold = float(self.params.get("min_spread") or self.threshold)
        is_opportunity = (self.funding_rate >= effective_threshold) or (spread >= effective_threshold)
        
        self.logger.info(f"📊 Check: Spread={spread:.6f} | Funding={self.funding_rate:.6f} | Thresh={effective_threshold} | Match={is_opportunity}")
        
        if not self.is_active and is_opportunity:
            self.logger.info(f"✅ Arb Opportunity! Funding: {self.funding_rate:.6f} | Spread: {spread:.6f} | Thresh: {effective_threshold}")
            self.is_active = True
            return [
                self.create_order(self.base_symbol, "buy", 0.1, {"leg": "entry"}),
                self.create_order(self.perp_symbol, "sell", 0.1, {"leg": "entry"})
            ]
        elif self.is_active and self.funding_rate < 0:
            self.logger.info(f"🛑 Closing Arb. Funding turned negative: {self.funding_rate:.6f}")
            self.is_active = False
            return [
                self.create_order(self.base_symbol, "sell", 0.1, {"leg": "exit"}),
                self.create_order(self.perp_symbol, "buy", 0.1, {"leg": "exit"})
            ]
        return None

if __name__ == "__main__":
    runner = GenericRunnerV3(CashAndCarryStrategy)
    asyncio.run(runner.run())
