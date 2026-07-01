import os
import sys
import asyncio
import json
import logging
import nats
from datetime import datetime
from typing import Dict

# Importar Base agregando la ruta al path para evitar paquetes que empiezan con numeros
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../services/23_execution_engine'))
sys.path.append(base_path)

try:
    from strategy_base import StrategyBase
except ImportError:
    # Fallback para entorno de produccion donde las rutas pueden cambiar
    class StrategyBase: 
        def __init__(self, **kwargs): pass
        async def start(self): pass
        async def publish_signal(self, k, v): pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CEX-DEX-ARB] %(message)s")
logger = logging.getLogger("Strategy14")

class FlashLoanArbStrategy(StrategyBase):
    """
    Estrategia 14: Arbitraje CEX vs DEX con Flash Loans.
    """
    def __init__(self):
        super().__init__(strategy_id="14", symbol="BTCUSDT")
        self.dex_price = 0.0
        self.cex_price = 0.0
        self.min_spread_pct = 0.005 
        self.rpc_url = os.getenv("ETH_RPC_URL", "https://eth-mainnet.g.alchemy.com/v2/placeholder")

    async def on_tick(self, tick: Dict):
        lp = tick.get("last", 0)
        self.cex_price = float(lp) if lp else 0.0
        await self.check_arbitrage()

    async def on_dex_update(self, price: float):
        self.dex_price = float(price)
        await self.check_arbitrage()

    async def check_arbitrage(self):
        if self.cex_price > 0 and self.dex_price > 0:
            spread = (self.cex_price / self.dex_price) - 1
            if spread > self.min_spread_pct:
                await self.execute_flash_loan("DEX_TO_CEX", volume=1.0)
            elif spread < -self.min_spread_pct:
                await self.execute_flash_loan("CEX_TO_DEX", volume=1.0)

    async def execute_flash_loan(self, direction: str, volume: float):
        msg = {
            "strategy": "14",
            "action": direction,
            "volume": volume,
            "cex_px": self.cex_price,
            "dex_px": self.dex_price,
            "ts": datetime.now().isoformat()
        }
        await self.publish_signal("arbitrage.execution", msg)
        logger.info(f"⚡ FLASH LOAN DISPARADO: {direction} | Vol: {volume}")

    async def start_dex_listener(self):
        while True:
            if self.cex_price > 0:
                self.dex_price = self.cex_price * (1 + (datetime.now().second % 10 - 5) / 1000)
            await asyncio.sleep(1)

async def main():
    bot = FlashLoanArbStrategy()
    asyncio.create_task(bot.start_dex_listener())
    await bot.start()

if __name__ == "__main__":
    asyncio.run(main())
