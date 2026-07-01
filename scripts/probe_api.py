import asyncio, websockets, json, time
from datetime import datetime, timezone

APP_ID = "123978"
TOKEN  = "OYGEaNuzcbkCIQk"
WS_URL = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"

SYMBOLS = ["R_75", "R_100", "R_50", "R_25", "R_10"]

async def probe():
    print("=" * 60)
    print("DERIV API PROBE — Límites y datos disponibles")
    print("=" * 60)

    async with websockets.connect(WS_URL, ping_interval=20) as ws:
        # Auth
        await ws.send(json.dumps({"authorize": TOKEN}))
        auth = json.loads(await ws.recv())
        if "error" in auth:
            print(f"ERROR auth: {auth['error']}")
            return
        print(f"✅ Auth OK — cuenta: {auth.get('authorize', {}).get('email', 'n/a')}")
        print()

        # 1. Máx ticks por request (raw ticks)
        for count in [500, 5000]:
            await ws.send(json.dumps({
                "ticks_history": "R_75",
                "count": count,
                "end": "latest",
                "style": "ticks"
            }))
            r = json.loads(await ws.recv())
            if "error" in r:
                print(f"  ticks count={count}: ERROR {r['error']}")
                continue
            times = r.get("history", {}).get("times", [])
            if times:
                t0 = datetime.fromtimestamp(times[0], tz=timezone.utc)
                t1 = datetime.fromtimestamp(times[-1], tz=timezone.utc)
                span_h = (times[-1] - times[0]) / 3600
                freq   = (times[-1] - times[0]) / max(len(times)-1, 1)
                print(f"Ticks count={count:5d} → got {len(times):5d} "
                      f"| span={span_h:.1f}h | freq={freq:.1f}s/tick")
                print(f"  desde: {t0.strftime('%Y-%m-%d %H:%M')} UTC")
                print(f"  hasta: {t1.strftime('%Y-%m-%d %H:%M')} UTC")

        print()

        # 2. Rango histórico con start explícito
        for days_back in [7, 30, 180, 365, 730]:
            start_ep = int(time.time()) - days_back * 86400
            await ws.send(json.dumps({
                "ticks_history": "R_75",
                "start": start_ep,
                "end": "latest",
                "count": 1,
                "style": "ticks"
            }))
            r = json.loads(await ws.recv())
            if "error" in r:
                print(f"  start={days_back:4d}d atrás: ERROR {r['error'].get('message','?')}")
            else:
                times = r.get("history", {}).get("times", [])
                if times:
                    t = datetime.fromtimestamp(times[0], tz=timezone.utc)
                    print(f"  start={days_back:4d}d atrás: primer tick={t.strftime('%Y-%m-%d %H:%M')} UTC ✅")
        print()

        # 3. Granularidades de candles
        print("Candles disponibles (count=5000):")
        for gran, label in [(60,"1m"),(300,"5m"),(3600,"1h"),(86400,"1d")]:
            await ws.send(json.dumps({
                "ticks_history": "R_75",
                "count": 5000,
                "end": "latest",
                "style": "candles",
                "granularity": gran
            }))
            r = json.loads(await ws.recv())
            if "error" in r:
                print(f"  {label:4s}: ERROR")
                continue
            candles = r.get("candles", [])
            if candles:
                t0 = datetime.fromtimestamp(candles[0]["epoch"], tz=timezone.utc)
                t1 = datetime.fromtimestamp(candles[-1]["epoch"], tz=timezone.utc)
                span_d = (candles[-1]["epoch"] - candles[0]["epoch"]) / 86400
                print(f"  {label:4s}: {len(candles):5d} candles | "
                      f"span={span_d:.0f}d | "
                      f"desde={t0.strftime('%Y-%m-%d')}")

        print()

        # 4. Todos los símbolos disponibles R_xx
        print("Símbolos volátiles disponibles:")
        for sym in SYMBOLS:
            await ws.send(json.dumps({
                "ticks_history": sym,
                "count": 1,
                "end": "latest",
                "style": "ticks"
            }))
            r = json.loads(await ws.recv())
            if "error" in r:
                print(f"  {sym}: ❌ {r['error'].get('message','?')}")
            else:
                times = r.get("history", {}).get("times", [])
                prices = r.get("history", {}).get("prices", [])
                if times:
                    t = datetime.fromtimestamp(times[-1], tz=timezone.utc)
                    print(f"  {sym:8s}: ✅ last={prices[-1]:.4f} @ {t.strftime('%H:%M:%S')} UTC")

asyncio.run(probe())
