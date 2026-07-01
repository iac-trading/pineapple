from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from uuid import UUID

@dataclass
class Tick:
    ts: str
    broker: str
    symbol: str
    bid: float
    ask: float
    last: float
    meta: Dict[str, Any] = field(default_factory=dict)

@dataclass
class OrderSubmit:
    instance_id: Optional[UUID]
    correlation_id: UUID
    side: str
    qty: float
    symbol: str
    order_type: str = "market"
    ts: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)
