import sys
import os
import json
import asyncio
import logging
from datetime import datetime, timezone
from uuid import uuid4

# Asegurar que importamos los modelos comunes de la plataforma
sys.path.append("/home/ansible/platform/strategies/common")
try:
    from models import Tick, OrderSubmit
    from strategy_base import StrategyBase
except ImportError:
    # Fallback/Mock for local testing
    from dataclasses import dataclass, field
    from uuid import UUID
    @dataclass
    class Tick:
        symbol: str; ts: str; px: float; volume: float = 0; bid: float = 0; ask: float = 0
    @dataclass
    class OrderSubmit:
        instance_id: UUID; correlation_id: UUID; side: str; qty: float; symbol: str; ts: str; meta: dict = field(default_factory=dict)
    class StrategyBase:
        def __init__(self, instance_id: str, params: dict):
            self.instance_id = UUID(instance_id) if isinstance(instance_id, str) else instance_id
            self.params = params
            self.logger = logging.getLogger(self.__class__.__name__)
        def create_order(self, symbol: str, side: str, qty: float, meta: dict = None) -> OrderSubmit:
            return OrderSubmit(instance_id=self.instance_id, correlation_id=uuid4(), side=side, qty=qty, symbol=symbol, ts=datetime.now(timezone.utc).isoformat(), meta=meta or {})

class L2ScalperStrategy(StrategyBase):
    """
    Estrategia 25: L2 Scalper (OFI + Sentiment)
    """
    def __init__(self, instance_id: str, params: dict):
        super().__init__(instance_id, params)
        self.symbol = params.get("symbol", "BTCUSDT")
        self.current_ofi = 0.0
        self.current_vpin = 0.0
        self.current_sentiment = 0.0
        self.last_order_ts = 0
        self.min_order_interval = params.get("min_order_interval", 5) # seconds
        
        self.ofi_threshold = params.get("ofi_threshold", 50)
        self.sent_threshold = params.get("sent_threshold", 0.2)
        self.vpin_limit = params.get("vpin_limit", 0.8)

    async def start(self, nc):
        """Override start to subscribe to intelligence signals."""
        self.nc = nc
        self.logger.info(f"L2 Scalper Strategy started for {self.symbol}")

        # Listen to Microstructure signals
        await self.nc.subscribe(f"intelligence.vpin.{self.symbol}", cb=self.on_vpin_data)
        
        # Listen to Sentiment signals
        await self.nc.subscribe("intelligence.sentiment", cb=self.on_sentiment_data)

    async def on_vpin_data(self, msg):
        data = json.loads(msg.data.decode())
        self.current_ofi = data.get("ofi", 0.0)
        self.current_vpin = data.get("vpin", 0.0)
        
        # Scalping is triggered by microstructural imbalance
        await self.evaluate_and_trade()

    async def on_sentiment_data(self, msg):
        data = json.loads(msg.data.decode())
        self.current_sentiment = data.get("sentiment", 0.0)

    def on_tick(self, tick: Tick) -> list[OrderSubmit]:
        # Ticks can be used to update mid-price or internal state, 
        # but the decision matrix lives in the signal handlers.
        return []

    async def evaluate_and_trade(self):
        now = datetime.now(timezone.utc).timestamp()
        if now - self.last_order_ts < self.min_order_interval:
            return

        # Decision Matrix
        # 1. High imbalance + Positive Sentiment -> Buy
        if self.current_ofi > self.ofi_threshold and self.current_sentiment > self.sent_threshold and self.current_vpin < self.vpin_limit:
            self.logger.info(f"🚀 BUY Signal | OFI: {self.current_ofi:.2f}, Sent: {self.current_sentiment:.2f}")
            order = self.create_order(self.symbol, "buy", 0.01, meta={"strategy": "BP-25", "vpin": self.current_vpin})
            await self.nc.publish("orders.submit", json.dumps(order.__dict__, default=str).encode())
            self.last_order_ts = now
        
        # 2. High negative imbalance + Negative Sentiment -> Sell
        elif self.current_ofi < -self.ofi_threshold and self.current_sentiment < -self.sent_threshold and self.current_vpin < self.vpin_limit:
            self.logger.info(f"🚀 SELL Signal | OFI: {self.current_ofi:.2f}, Sent: {self.current_sentiment:.2f}")
            order = self.create_order(self.symbol, "sell", 0.01, meta={"strategy": "BP-25", "vpin": self.current_vpin})
            await self.nc.publish("orders.submit", json.dumps(order.__dict__, default=str).encode())
            self.last_order_ts = now
        
        # 3. Toxicity Filter
        elif self.current_vpin > self.vpin_limit:
            self.logger.warning(f"⚠️ High Market Toxicity (VPIN: {self.current_vpin:.2f}). Blocking trades.")

if __name__ == "__main__":
    import nats
    async def main():
        nc = await nats.connect("nats://192.168.100.200:4222")
        bot = L2ScalperStrategy(str(uuid4()), {"symbol": "BTCUSDT"})
        await bot.start(nc)
        while True:
            await asyncio.sleep(1)
    
    asyncio.run(main())
