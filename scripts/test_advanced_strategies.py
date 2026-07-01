import asyncio
import json
import time
import uuid
from nats.aio.client import Client as NATS
from datetime import datetime, timezone

NATS_URL = os.environ["NATS_URL"]

async def simulate_market_data():
    nc = NATS()
    await nc.connect(servers=[NATS_URL])
    print(f"Connected to NATS at {NATS_URL}")

    # Suscribirse a órdenes para ver si la estrategia responde
    async def order_cb(msg):
        data = json.loads(msg.data.decode())
        print(f"\n[RECEIVED ORDER] {data['side']} {data['qty']} {data['symbol']} Corr={data['correlation_id']}")
        print(f"Meta: {data.get('meta')}")

    await nc.subscribe("orders.submit", cb=order_cb)

    print("--- Simulating Pairs Trading Data (SYM_A & SYM_B) ---")
    print("Goal: Create a spread that diverges and then converges.")

    base_price = 100.0
    
    # Enviar 500 ticks para llenar el buffer (necesario para la regresión)
    for i in range(510):
        ts = datetime.now(timezone.utc).isoformat()
        
        # SYM_A sigue a SYM_B con un poco de ruido
        price_b = base_price + (i * 0.01) # Tendencia leve alcista
        
        # Forzar una anomalía en los últimos ticks para disparar el Z-Score
        spread_noise = 0.0
        if i > 500:
            spread_noise = 5.0 # SYM_A sube artificialmente respecto a B
            
        price_a = price_b + spread_noise 

        # Publicar Tick A
        tick_a = {
            "ts": ts, "broker": "binance", "symbol": "SYM_A",
            "bid": price_a - 0.01, "ask": price_a + 0.01, "last": price_a
        }
        await nc.publish("md.binance.SYM_A.tick", json.dumps(tick_a).encode())

        # Publicar Tick B
        tick_b = {
            "ts": ts, "broker": "binance", "symbol": "SYM_B",
            "bid": price_b - 0.01, "ask": price_b + 0.01, "last": price_b
        }
        await nc.publish("md.binance.SYM_B.tick", json.dumps(tick_b).encode())

        if i % 100 == 0:
            print(f"Injected {i} ticks...")
        
        if i > 500:
            await asyncio.sleep(0.5) # Ir más lento al final para ver la reacción

    print("\n[DONE] Simulation finished. If Strategy 19 was running with SYM_A/SYM_B, you should have seen orders above.")
    await nc.close()

if __name__ == "__main__":
    asyncio.run(simulate_market_data())
