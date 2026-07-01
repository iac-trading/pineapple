import os
import json
import websockets
from datetime import datetime, timezone
from .models import Tick

# CAMBIO MAESTRO: Actualizamos a ws.derivws.com para evitar el error de 0 bytes
DERIV_WS_URL = os.getenv("DERIV_WS_URL", "wss://ws.derivws.com/websockets/v3")

async def deriv_ticks_stream(app_id: str, token: str, symbol: str):
    url = f"{DERIV_WS_URL}?app_id={app_id}"
    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(json.dumps({"authorize": token}))
        auth_resp = json.loads(await ws.recv())
        if "error" in auth_resp:
            raise RuntimeError(f"Deriv auth error: {auth_resp['error']}")

        await ws.send(json.dumps({"ticks": symbol, "subscribe": 1}))
        while True:
            msg = json.loads(await ws.recv())
            if msg.get("msg_type") != "tick":
                continue

            t = msg["tick"]
            ts_iso = datetime.fromtimestamp(t.get("epoch"), tz=timezone.utc).isoformat()

            yield Tick(
                ts=ts_iso,
                broker="deriv",
                symbol=symbol,
                bid=t.get("bid"),
                ask=t.get("ask"),
                last=t.get("quote"),
                meta={"id": t.get("id"), "epoch": t.get("epoch")}
            ).model_dump()