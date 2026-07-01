import asyncio
import json
import uuid
import nats
import os

NATS_URL = os.environ["NATS_URL"]

async def test_risk_flow():
    print(f"Connecting to NATS at {NATS_URL}...")
    nc = await nats.connect(NATS_URL)
    
    # 0. Warm up Price Cache (Injecting fake MD)
    print("\n[PRE-TEST] Injecting Price Cache data...")
    md_spy = {"symbol": "SPY", "last": 500.0, "ts": "2026-03-16T16:58:00Z", "broker": "yfinance"}
    md_btc = {"symbol": "BTCUSDT", "last": 60000.0, "ts": "2026-03-16T16:58:00Z", "broker": "binance"}
    await nc.publish("md.yfinance.SPY.tick", json.dumps(md_spy).encode())
    await nc.publish("md.binance.BTCUSDT.tick", json.dumps(md_btc).encode())
    await asyncio.sleep(0.5) # Give Risk Engine time to cache

    # 1. Test Passing Order (SPY is ~$500, so 10 qty is $5k)
    correlation_id_pass = str(uuid.uuid4())
    payload_pass = {
        "instance_id": str(uuid.uuid4()),
        "correlation_id": correlation_id_pass,
        "symbol": "SPY",
        "side": "BUY",
        "qty": 10.0,
        "ts": "2026-03-16T16:58:00Z",
        "meta": {"broker": "yfinance"}
    }
    
    print(f"\n[TEST 1] Sending AUTHORIZED intent (SPY)...")
    await nc.publish("orders.intent", json.dumps(payload_pass).encode())
    
    # 2. Test Rejected Order (Leverage Guard - BTCUSDT is ~$60k, so 100 qty is $6M)
    correlation_id_fail = str(uuid.uuid4())
    payload_fail = {
        "instance_id": str(uuid.uuid4()),
        "correlation_id": correlation_id_fail,
        "symbol": "BTCUSDT",
        "side": "BUY",
        "qty": 100.0,
        "ts": "2026-03-16T16:58:00Z",
        "meta": {"broker": "binance"}
    }
    
    print(f"[TEST 2] Sending REJECTED (Leverage) intent (BTCUSDT)...")
    await nc.publish("orders.intent", json.dumps(payload_fail).encode())

    await nc.flush()
    await nc.close()
    print("\n[OK] Test intents sent. Check 'sudo docker logs risk_engine' for results.")

if __name__ == "__main__":
    asyncio.run(test_risk_flow())
