import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID, uuid5, NAMESPACE_DNS

@dataclass
class Tick:
    ts: str
    broker: str
    symbol: str
    bid: float
    ask: float
    last: float
    meta: Dict[str, Any] = field(default_factory=dict)

    def model_dump(self) -> Dict[str, Any]:
        return {
            "ts": self.ts, "broker": self.broker, "symbol": self.symbol,
            "bid": self.bid, "ask": self.ask, "last": self.last, "meta": self.meta
        }

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _uuid_from_any(v: Any, field_name: str) -> UUID:
    if v is None: raise ValueError(f"Missing field: {field_name}")
    s = str(v).strip()
    try: return UUID(s)
    except Exception: return uuid5(NAMESPACE_DNS, f"{field_name}:{s}")

def _as_float(v: Any, field_name: str) -> float:
    if v is None: raise ValueError(f"Missing field: {field_name}")
    try: return float(v)
    except Exception: raise ValueError(f"Invalid float for {field_name}: {v}")

def _normalize_side(v: Any) -> str:
    if v is None: raise ValueError("Missing field: side")
    side = str(v).strip().lower()
    if side in ("buy", "long"): return "buy"
    if side in ("sell", "short"): return "sell"
    raise ValueError(f"Invalid side: {v}")

@dataclass
class OrderSubmit:
    instance_id: Optional[UUID]
    correlation_id: UUID
    side: str
    qty: float
    order_type: str = "market"
    ts: Optional[str] = None
    bot_id: Optional[str] = None
    symbol: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

@dataclass
class OrderEvent:
    ts: str
    instance_id: Optional[UUID]
    correlation_id: UUID
    event_type: str
    status: str
    broker: str
    symbol: str
    side: str
    qty: float
    execution_price: Optional[float] = None
    broker_order_id: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)

    def model_dump(self) -> Dict[str, Any]:
        return {
            "ts": self.ts, "instance_id": str(self.instance_id) if self.instance_id else None,
            "correlation_id": str(self.correlation_id), "event_type": self.event_type,
            "status": self.status, "broker": self.broker, "symbol": self.symbol,
            "side": self.side, "qty": self.qty, "execution_price": self.execution_price,
            "broker_order_id": self.broker_order_id, "payload": self.payload,
        }

@dataclass
class Position:
    broker: str
    symbol: str
    qty: float
    avg_price: float = 0.0
    side: str = "long" # long/short
    payload: Dict[str, Any] = field(default_factory=dict)

    def model_dump(self) -> Dict[str, Any]:
        return {
            "broker": self.broker, "symbol": self.symbol,
            "qty": self.qty, "avg_price": self.avg_price,
            "side": self.side, "payload": self.payload
        }