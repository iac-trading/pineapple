import asyncio
import json
import random
import time
from nats.aio.client import Client as NATS

import os
NATS_URL = os.environ["NATS_URL"]

async def simulate_l2(symbol="BTCUSDT"):
    nc = NATS()
    await nc.connect(servers=[NATS_URL])
    print(f"L2 Simulator started for {symbol} on {NATS_URL}")

    base_price = 65000.0
    
    while True:
        # Simulate price movement
        base_price += random.uniform(-5, 5)
        
        # Generate Order Book
        bids = []
        asks = []
        for i in range(5):
            bids.append([base_price - (i * 0.5) - random.uniform(0, 0.2), random.uniform(0.1, 2.0)])
            asks.append([base_price + (i * 0.5) + random.uniform(0, 0.2), random.uniform(0.1, 2.0)])
        
        payload = {
            "symbol": symbol,
            "ts": int(time.time() * 1000),
            "bids": bids, # [[price, size], ...]
            "asks": asks,
            "seq": int(time.time() * 100)
        }
        
        subject = f"md.binance.{symbol}.l2"
        await nc.publish(subject, json.dumps(payload).encode())
        
        if random.random() > 0.8:
            print(f"Published L2 update for {symbol} | Mid: {base_price:.2f}")
            
        await asyncio.sleep(0.5) # 2 updates per second

if __name__ == "__main__":
    try:
        asyncio.run(simulate_l2())
    except KeyboardInterrupt:
        print("Simulator stopped.")
