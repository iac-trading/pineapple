import os
import sys
from typing import Optional, Dict, Any

# Asegurar que las clases base estén en el path
sys.path.append(os.path.join(os.path.dirname(__file__), "../../common"))
from models import Tick, OrderSubmit
from strategy_base import StrategyBase
from runner_v3 import GenericRunnerV3
from filters import RegimeFilter

class TemplateStrategy(StrategyBase):
    def __init__(self, instance_id: str, params: Dict[str, Any]):
        super().__init__(instance_id, params)
        self.hurst_threshold = float(params.get("hurst_threshold", 0.55))
        self.hurst_window = int(params.get("hurst_window", 500))

    def on_tick(self, tick: Tick) -> Optional[OrderSubmit]:
        # 1. Almacenar tick
        self._add_to_history(tick)
        
        hist = self.history.get(tick.symbol, [])
        if len(hist) < self.hurst_window:
            return None
            
        # 2. Aplicar Escudo de Infraestructura (Hurst Exponent)
        prices = [t.last for t in hist[-self.hurst_window:]]
        
        # Solo operamos si el mercado tiene tendencia (H > threshold)
        if not RegimeFilter.is_trending(prices, window=self.hurst_window, threshold=self.hurst_threshold):
            # Mercado en rango o sin tendencia clara -> Filtramos operaciones
            return None
            
        # 3. Lógica Propietaria (Ejemplo: Cruce de Medias o Breakout)
        self.logger.info(f"📊 Mercado en Tendencia (Hurst OK). Ejecutando lógica...")
        
        # ... Lógica aquí ...
        
        return None

if __name__ == "__main__":
    import asyncio
    NATS_URL = os.getenv("NATS_URL", "nats://nats:4222")
    runner = GenericRunnerV3(TemplateStrategy, NATS_URL)
    asyncio.run(runner.run())
