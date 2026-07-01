import asyncio
import logging
from ib_insync import IB, Forex, Stock, util

logger = logging.getLogger("IbkrClient")

class IbkrClient:
    def __init__(self, host: str, broker_id: str = "ibkr_paper", client_id: int = 1):
        self.host = host
        # Live: 7496, Paper: 4001 (default)
        self.port = 7496 if broker_id == "ibkr_live" else 4001
        self.client_id = client_id
        self.ib = IB()
        self._last_order_ts = 0
        logger.info(f"IbkrClient ({broker_id}): Port {self.port} selected. Rate limiter initialized.")

    async def connect(self):
        if self.ib.isConnected():
            return
        
        try:
            logger.info(f"IBKR: Connecting to {self.host}:{self.port}")
            await self.ib.connectAsync(self.host, self.port, clientId=self.client_id)
            logger.info("IBKR: Connected successfully.")
        except Exception as e:
            logger.error(f"IBKR Connection failed: {e}")
            raise

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()

    async def _rate_limit_wait(self):
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_order_ts
        if elapsed < 0.2: # 200ms safety for IBKR
            await asyncio.sleep(0.2 - elapsed)
        self._last_order_ts = asyncio.get_event_loop().time()

    async def place_market_order(self, symbol: str, side: str, qty: float):
        await self.connect()
        await self._rate_limit_wait()
        
        # Simple heuristic: EURUSD -> Forex, others -> Stock (placeholder logic)
        if len(symbol) == 6 and symbol.isupper():
            base, quote = symbol[:3], symbol[3:]
            contract = Forex(f"{base}.{quote}", 'IDEALPRO', 'USD')
        else:
            contract = Stock(symbol, 'SMART', 'USD')

        logger.info(f"IBKR: Placing market {side} order for {contract} qty={qty}")
        
        order_side = "BUY" if side.lower() in ("buy", "long") else "SELL"
        order = util.marketOrder(order_side, qty)
        
        trade = self.ib.placeOrder(contract, order)
        
        # Wait for fill or status change
        while not trade.isDone():
            await asyncio.sleep(0.5)
            
        status = str(trade.orderStatus.status).lower()
        # Map statuses
        if status == "filled":
            status = "filled"
        elif status in ("submitted", "presubmitted"):
            status = "partially_filled" if trade.orderStatus.filled > 0 else "open"
        elif status == "inactive":
            status = "rejected"

        return {
            "broker_order_id": str(trade.order.orderId),
            "status": status,
            "price": float(trade.orderStatus.avgFillPrice or 0.0),
            "broker": "ibkr",
            "raw": {"orderId": trade.order.orderId, "permId": trade.order.permId, "filled": trade.orderStatus.filled, "remaining": trade.orderStatus.remaining}
        }

    async def get_positions(self):
        await self.connect()
        # Fetch current positions
        positions = self.ib.positions()
        res = []
        for pos in positions:
            res.append({
                "symbol": pos.contract.localSymbol or pos.contract.symbol,
                "qty": float(pos.position),
                "avg_price": float(pos.avgCost),
                "broker": "ibkr",
                "raw": {"account": pos.account, "conId": pos.contract.conId}
            })
        return res
