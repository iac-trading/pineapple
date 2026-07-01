import os
import json
import logging
import asyncio
import numpy as np
from typing import Dict, Any, Optional, List
from datetime import datetime

from common.models import Tick, OrderSubmit
from common.strategy_base import StrategyBase

logging.basicConfig(level=logging.INFO, format="%(asctime)s [MM-ST37] %(message)s")
logger = logging.getLogger("MarketMakerAS")

NATS_URL = os.environ["NATS_URL"]

class MarketMakerStrategy(StrategyBase):
    """
    Estrategia 37: Market Making with Inventory Control (Avellaneda-Stoikov)
    
    Ajusta el precio de reserva y el spread óptimo basado en el inventario actual
    para mitigar el riesgo de inventario.
    """
    def __init__(self, instance_id: str, params: Dict[str, Any]):
        super().__init__(instance_id, params)
        
        self.symbol = params.get("symbol", "BTCUSDT")
        
        # AS Model Parameters
        self.gamma = float(params.get("gamma", 0.1))    # Risk aversion
        self.sigma = float(params.get("sigma", 0.02))   # Volatility
        self.kappa = float(params.get("kappa", 1.5))    # Order intensity
        self.q_max = float(params.get("q_max", 10.0))   # Max inventory units
        
        # State
        self.inventory = 0.0
        self.last_mid = 0.0

    def on_tick(self, tick: Tick) -> Optional[List[OrderSubmit]]:
        """
        Calcula cotizaciones Bid/Ask basadas en el modelo AS cuando llega L2 Depth.
        """
        self._add_to_history(tick)
        
        # Solo operamos si tenemos precios Bid/Ask válidos (Representando el Mid)
        if not tick.bid or not tick.ask:
            return None
            
        mid = (tick.bid + tick.ask) / 2.0
        self.last_mid = mid
        
        # 1. Calculate Reservation Price (r)
        # reservation_price = s - q * gamma * sigma^2
        reservation_price = mid - (self.inventory * self.gamma * (self.sigma ** 2))
        
        # 2. Calculate Optimal Spread (delta)
        # delta = (2/gamma) * ln(1 + gamma/kappa)
        spread = (2 / self.gamma) * np.log(1 + (self.gamma / self.kappa))
        
        bid_quote = reservation_price - (spread / 2)
        ask_quote = reservation_price + (spread / 2)
        
        # 3. Safety: Inventory constraints
        orders = []
        
        # Emitimos cotizaciones como órdenes LIMIT (El executor las gestionará como Quotes)
        if self.inventory < self.q_max:
            orders.append(self.create_order(
                symbol=self.symbol,
                side="buy", # Bid
                qty=1.0,
                meta={"price": round(bid_quote, 2), "type": "QUOTE", "is_post_only": True}
            ))
            
        if self.inventory > -self.q_max:
            orders.append(self.create_order(
                symbol=self.symbol,
                side="sell", # Ask
                qty=1.0,
                meta={"price": round(ask_quote, 2), "type": "QUOTE", "is_post_only": True}
            ))
            
        # Nota: En MM real, actualizaríamos el inventario vía on_fill. 
        # StrategyBase asume que el inventario se gestiona externamente o vía fills.
        return orders

    def on_fill(self, fill_data: Dict[str, Any]):
        """Callback específico para actualizar inventario desde el Runner."""
        side = fill_data.get("side", "").upper()
        qty = float(fill_data.get("qty", 0.0))
        if side == "BUY":
            self.inventory += qty
        else:
            self.inventory -= qty
        self.logger.info(f"Inventory Updated: {self.inventory:.4f}")

if __name__ == "__main__":
    # Inyectado por Runner V3
    pass
