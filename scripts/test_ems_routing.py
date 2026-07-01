import asyncio
import json
import uuid
import nats
import os

NATS_URL = os.environ["NATS_URL"]

async def test_ems_routing():
    print(f"Connecting to NATS at {NATS_URL}...")
    nc = await nats.connect(NATS_URL)

    # Note: Replace with a real active instanceid from your DB if you want to test full flow
    # Otherwise, this will trigger the 'instance not found' journal entry which confirms routing works
    test_instance_id = "550e8400-e29b-41d4-a716-446655440000" 

    brokers = ["binance", "ibkr", "deriv"]
    
    for broker in brokers:
        correlation_id = str(uuid.uuid4())
        payload = {
            "instance_id": test_instance_id,
            "correlation_id": correlation_id,
            "side": "buy",
            "qty": 0.001,
            "meta": {"broker": broker, "test": True}
        }
        
        print(f"\n[TEST] Sending order to EMS for broker: {broker.upper()}")
        await nc.publish("orders.submit", json.dumps(payload).encode())
        print(f"Published to orders.submit (correlation_id: {correlation_id})")

    await nc.flush()
    await nc.close()
    print("\n[OK] All test messages sent. Check 'sudo docker logs -f bridge_executor' to see the routing logic in action.")

if __name__ == "__main__":
    asyncio.run(test_ems_routing())
