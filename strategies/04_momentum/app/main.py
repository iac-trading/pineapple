import sys
import os
import json
import logging
import asyncio
from typing import Optional
from datetime import datetime, timezone, timedelta

# Asegurar que importamos los modelos comunes de la plataforma
sys.path.append("/home/ansible/platform/strategies/common")
try:
    from models import Tick, OrderSubmit
    from strategy_base import StrategyBase
except ImportError:
    # Fallback/Mock for local testing without the broader environment
    from dataclasses import dataclass, field
    from uuid import UUID, uuid4
    @dataclass
    class Tick:
        symbol: str; ts: str; px: float; volume: float = 0; bid: float = 0; ask: float = 0
    @dataclass
    class OrderSubmit:
        instance_id: UUID; correlation_id: UUID; side: str; qty: float; symbol: str; ts: str; meta: dict = field(default_factory=dict)
    class StrategyBase:
        def __init__(self, instance_id: str, params: dict):
            self.instance_id = UUID(instance_id)
            self.params = params
            self.logger = logging.getLogger(self.__class__.__name__)
        def create_order(self, symbol: str, side: str, qty: float, meta: dict = None) -> OrderSubmit:
            return OrderSubmit(instance_id=self.instance_id, correlation_id=uuid4(), side=side, qty=qty, symbol=symbol, ts=datetime.now(timezone.utc).isoformat(), meta=meta or {})

class MomentumStrategy(StrategyBase):
    """
    Estrategia 04: Cross-Sectional Momentum (Live Execution)
    
    A diferencia de las estrategias de alta frecuencia, el Momentum evalúa
    señales diarias calculadas por el pipeline de Inteligencia (DAG de Airflow).
    Esta clase lee el archivo local de señales y emite las órdenes necesarias
    cuando detecta un cambio en el archivo o recibe el primer tick del día.
    """
    def __init__(self, instance_id: str, params: dict):
        super().__init__(instance_id, params)
        self.signals_path = params.get("signals_path", "/home/ansible/platform/signals/momentum_daily.json")
        self.allocation_per_leg = params.get("allocation_per_leg", 10000) # $ USD por símbolo
        
        self.last_signal_ts = None
        self.current_longs = set()
        self.current_shorts = set()
        self.logger.info(f"Initialized Momentum Execution Bot. Watching: {self.signals_path}")

    def _load_signals(self) -> dict:
        """Lee el último JSON generado por el Airflow DAG (momentum_analyzer)."""
        if not os.path.exists(self.signals_path):
            return None
        try:
            with open(self.signals_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error reading signals: {e}")
            return None

    def on_tick(self, tick: Tick) -> list[OrderSubmit]:
        orders = []
        
        # 1. Leer las señales diarias
        signal_data = self._load_signals()
        if not signal_data:
            return orders
            
        signal_ts = signal_data.get("ts")
        
        # 2. Rebalancear solo si la señal es nueva
        if signal_ts != self.last_signal_ts:
            self.logger.info(f"New Momentum Signal Detected (Generated at {signal_ts})")
            
            target_longs = set(signal_data.get("longs", []))
            target_shorts = set(signal_data.get("shorts", []))
            
            # --- Lógica de Cierre ---
            # Vender todo lo que era Long y ya no lo es
            to_close_longs = self.current_longs - target_longs
            for sym in to_close_longs:
                self.logger.info(f"Closing LONG position for {sym}")
                orders.append(self.create_order(sym, "sell", 0, meta={"action": "close_long"})) # qty=0 to close all according to OMS logic
                
            # Comprar de vuelta todo lo que era Short y ya no lo es
            to_close_shorts = self.current_shorts - target_shorts
            for sym in to_close_shorts:
                self.logger.info(f"Closing SHORT position for {sym}")
                orders.append(self.create_order(sym, "buy_to_cover", 0, meta={"action": "close_short"}))
                
            # --- Lógica de Apertura ---
            # Entrar en nuevos Longs
            to_open_longs = target_longs - self.current_longs
            for sym in to_open_longs:
                # Calculamos qty aproximada basándonos en el precio actual del tick 
                # Opcional: El OMS real calculará "qty_usd" si se pasa en los metadatos.
                self.logger.info(f"Opening LONG position for {sym}")
                orders.append(self.create_order(sym, "buy", 1.0, meta={"qty_usd": self.allocation_per_leg}))
                
            # Entrar en nuevos Shorts
            to_open_shorts = target_shorts - self.current_shorts
            for sym in to_open_shorts:
                self.logger.info(f"Opening SHORT position for {sym}")
                orders.append(self.create_order(sym, "sell_short", 1.0, meta={"qty_usd": self.allocation_per_leg}))
                
            # Actualizar estado interno
            self.current_longs = target_longs
            self.current_shorts = target_shorts
            self.last_signal_ts = signal_ts
            
            self.logger.info(f"Rebalancing orders generated: {len(orders)}")
            
        return orders

# ---------------------------------------------------------------------------
# Loop de prueba independiente (si se ejecuta directamente en lugar de NATS)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    bot = MomentumStrategy(str(uuid4()), {})
    # Creamos un archivo de señal falso para probar
    os.makedirs(os.path.dirname(bot.signals_path), exist_ok=True)
    with open(bot.signals_path, 'w') as f:
         json.dump({"ts": "2026-03-14T10:00:00Z", "longs": ["AAPL", "MSFT"], "shorts": ["TSLA"]}, f)
    
    # Simulamos que llega un Tick cualquiera
    fake_tick = Tick(symbol="AAPL", ts=datetime.now().isoformat(), px=150.0)
    generated_orders = bot.on_tick(fake_tick)
    
    print(f"Generated {len(generated_orders)} orders:")
    for o in generated_orders:
        print(o)
