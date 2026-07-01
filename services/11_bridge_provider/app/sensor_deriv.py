import os
import asyncio
import json
import logging
from nats.aio.client import Client as NATS
from .deriv import deriv_ticks_stream

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SensorDeriv")

NATS_URL = os.getenv("NATS_URL", "nats://nats:4222")
SYMBOL = os.getenv("DERIV_SYMBOL", "R_75")

async def run_sensor():
    nc = NATS()
    await nc.connect(servers=[NATS_URL])
    logger.info(f"✅ Conectado a NATS={NATS_URL}. Capturando ticks de {SYMBOL}...")

    app_id = os.getenv("DERIV_APP_ID")
    token = os.getenv("DERIV_TOKEN")
    if not app_id or not token:
        logger.error("DERIV_APP_ID/DERIV_TOKEN no están configurados.")
        return

    async for tick in deriv_ticks_stream(app_id, token, SYMBOL):
        subject = f"md.deriv.{SYMBOL}.tick"
        await nc.publish(subject, json.dumps(tick).encode())

if __name__ == "__main__":
    asyncio.run(run_sensor())