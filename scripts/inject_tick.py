import asyncio
import json
import nats
from datetime import datetime
import os

# NATS credentials from standard platform pattern
NATS_URL = os.environ["NATS_URL"]

async def main():
    print(f"Connecting to NATS at {NATS_URL}...")
    try:
        nc = await nats.connect(NATS_URL)
    except Exception as e:
        print(f"Failed to connect: {e}")
        return

    # Simulate BTCUSDT (Spot) and BTCUSDT-PERP (Futures)
    # We set a large spread to guarantee the strategy triggers
    ticks = [
        {
            "ts": datetime.utcnow().isoformat() + "Z",
            "broker": "binance",
            "symbol": "BTCUSDT",
            "bid": 65000.0,
            "ask": 65001.0,
            "last": 65000.5
        },
        {
            "ts": datetime.utcnow().isoformat() + "Z",
            "broker": "binance",
            "symbol": "BTCUSDT-PERP",
            "bid": 66000.0,
            "ask": 66001.0,
            "last": 66000.5
        },
        {
            "ts": datetime.utcnow().isoformat() + "Z",
            "broker": "deriv",
            "symbol": "R_75",
            "bid": 250000.0,
            "ask": 250005.0,
            "last": 250002.5
        }
    ]

    for t in ticks:
        subject = f"md.{t['broker']}.{t['symbol']}.tick"
        await nc.publish(subject, json.dumps(t).encode())
        print(f"✅ Injected tick: {subject} | Price: {t['last']}")

    await nc.flush()
    await nc.close()
    print("Injection complete.")

if __name__ == "__main__":
    asyncio.run(main())
