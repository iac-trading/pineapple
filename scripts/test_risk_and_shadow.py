import asyncio
import json
import uuid
import nats
import os

NATS_URL = os.environ["NATS_URL"]

async def test_risk_and_shadow():
    nc = await nats.connect(NATS_URL)
    print(f"Connected to NATS at {NATS_URL}")

    # 1. TEST RISK ENGINE (Reject Case)
    correlation_id_risk = str(uuid.uuid4())
    intent_payload = {
        "instance_id": "550e8400-e29b-41d4-a716-446655440000",
        "correlation_id": correlation_id_risk,
        "side": "buy",
        "qty": 999.0,  # Exceeds MAX_QTY_PER_ORDER = 100.0
        "symbol": "BTC-USD",
        "meta": {"broker": "binance"}
    }
    print(f"\n[RISK TEST] Sending Intent with Qty=999 (Should be REJECTED)")
    await nc.publish("orders.intent", json.dumps(intent_payload).encode())

    # 2. TEST RISK ENGINE (Pass Case)
    correlation_id_pass = str(uuid.uuid4())
    intent_pass = intent_payload.copy()
    intent_pass["correlation_id"] = correlation_id_pass
    intent_pass["qty"] = 1.0  # Valid
    print(f"[RISK TEST] Sending Intent with Qty=1.0 (Should be PROMOTED to orders.submit)")
    await nc.publish("orders.intent", json.dumps(intent_pass).encode())

    # 3. TEST SHADOW TRADING
    # Note: Requires an instance marked as is_shadow=true in DB
    # For this CLI test, we simulate the 'orders.submit' directly as if risk engine allowed it
    correlation_id_shadow = str(uuid.uuid4())
    shadow_payload = {
        "instance_id": "550e8400-e29b-41d4-a716-446655440000",
        "correlation_id": correlation_id_shadow,
        "side": "buy",
        "qty": 0.5,
        "symbol": "ETH-USD",
        "meta": {"broker": "binance", "is_shadow": True} # Force shadow in meta for test
    }
    print(f"\n[SHADOW TEST] Sending Order directly to submit with is_shadow=True")
    await nc.publish("orders.submit", json.dumps(shadow_payload).encode())

    await nc.flush()
    print("\n[DONE] Messages sent. Monitor logs with:")
    print("  - Risk Engine:  sudo docker logs -f risk_engine")
    print("  - Bridge EMS:   sudo docker logs -f bridge_executor")
    
    await nc.close()

if __name__ == "__main__":
    asyncio.run(test_risk_and_shadow())
