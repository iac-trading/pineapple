import asyncio
import json
import uuid
import os
from nats.aio.client import Client as NATS

NATS_URL = os.getenv("NATS_URL", "nats://nats:4222")

async def test_risk_and_shadow():
    nc = NATS()
    await nc.connect(servers=[NATS_URL])
    
    # 1. Test Valid Order intent -> Should be promoted to orders.submit
    valid_order = {
        "instance_id": str(uuid.uuid4()),
        "correlation_id": f"test-valid-{uuid.uuid4().hex[:6]}",
        "symbol": "R_75",
        "side": "buy",
        "qty": 1.0,
        "meta": {"price": 1000.0}
    }
    
    # Listen for promotion
    submit_received = asyncio.Future()
    async def on_submit(msg):
        submit_received.set_result(json.loads(msg.data.decode()))
    
    sub = await nc.subscribe("orders.submit", cb=on_submit)
    
    print(f"Testing valid order: {valid_order['correlation_id']}...")
    await nc.publish("orders.intent", json.dumps(valid_order).encode())
    
    try:
        result = await asyncio.wait_for(submit_received, timeout=2.0)
        print(f"✅ Risk Engine promoted valid order: {result['correlation_id']}")
    except asyncio.TimeoutError:
        print("❌ Risk Engine FAILED to promote valid order.")

    await sub.unsubscribe()

    # 2. Test Fat Finger (Invalid Qty) -> Should be rejected
    invalid_order = {
        "instance_id": str(uuid.uuid4()),
        "correlation_id": f"test-invalid-{uuid.uuid4().hex[:6]}",
        "symbol": "R_75",
        "side": "buy",
        "qty": 500.0, # Exceeds default 100
        "meta": {"price": 1000.0}
    }
    
    reject_received = asyncio.Future()
    async def on_event(msg):
        data = json.loads(msg.data.decode())
        if data.get("event_type") == "ORDER_REJECTED" and data.get("correlation_id") == invalid_order["correlation_id"]:
            reject_received.set_result(data)

    event_sub = await nc.subscribe("orders.events", cb=on_event)
    
    print(f"Testing invalid order (qty 500): {invalid_order['correlation_id']}...")
    await nc.publish("orders.intent", json.dumps(invalid_order).encode())
    
    try:
        reject = await asyncio.wait_for(reject_received, timeout=2.0)
        print(f"✅ Risk Engine REJECTED invalid order: {reject['payload']['reason']}")
    except asyncio.TimeoutError:
        print("❌ Risk Engine FAILED to reject invalid order.")

    await event_sub.unsubscribe()
    await nc.close()

if __name__ == "__main__":
    asyncio.run(test_risk_and_shadow())
