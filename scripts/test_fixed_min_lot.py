import asyncio
import json
import uuid
import nats
import os

# Adaptar a la IP del servidor Brain (Orquestador)
NATS_URL = os.environ["NATS_URL"]

async def test_fixed_min_lot():
    print(f"Connecting to NATS at {NATS_URL}...")
    try:
        nc = await nats.connect(NATS_URL)
    except Exception as e:
        print(f"Error connecting to NATS: {e}")
        return
    
    symbol = "R_10" # Símbolo activo en el tablero del usuario
    
    # Payload de prueba: Intentamos enviar 500 lotes, pero el Risk Engine debe forzar 0.001
    # Usamos un Instance ID REAL que sepamos que existe (el de donchian_hurst_r10 es el ID 6)
    instance_id = "101e0000-0000-0000-0000-000000000006"
    
    payload = {
        "instance_id": instance_id,
        "correlation_id": str(uuid.uuid4()), # DEBE ser un UUID válido
        "symbol": symbol,
        "side": "BUY",
        "qty": 0.5, 
        "meta": {
            "size_model": "FIXED_MIN_LOT",
            "min_lot": 0.001,
            "broker": "deriv"
        }
    }
    
    print(f"\n[TEST] Enviando Intención de Orden (Qty {payload['qty']} -> esperado 0.001)")
    await nc.publish("orders.intent", json.dumps(payload).encode())
    
    await nc.flush()
    await nc.close()
    print("\n[OK] Intento de prueba enviado.")
    print("Próximo paso: Revisa los logs en el servidor compute con:")
    print("ansible compute -i deploy/ansible/inventory/proxmox/hosts.yml -m shell -a 'docker logs risk_engine --tail 20'")

if __name__ == "__main__":
    asyncio.run(test_fixed_min_lot())
