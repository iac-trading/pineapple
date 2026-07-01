import asyncio
import json
import os
import uuid
import argparse
from nats.aio.client import Client as NATS

async def trigger_backtest(args):
    nc = NATS()
    nats_url = os.environ["NATS_URL"]
    
    print(f"Connecting to NATS at {nats_url}...")
    await nc.connect(servers=[nats_url])
    
    job_id = str(uuid.uuid4())
    
    # Construir params dinámicos
    # Si viene via --params (JSON string), usar eso.
    # Si no, usar los argumentos individuales por compatibilidad.
    params = {}
    if args.params:
        try:
            params = json.loads(args.params)
        except Exception as e:
            print(f"Error parsing --params: {e}")
            return
    else:
        # Fallback a parámetros clásicos
        params = {
            "entry_p": args.entry,
            "exit_p": args.exit,
            "commission_pct": args.comm,
            "slippage_abs": args.slip
        }

    payload = {
        "job_id": job_id,
        "symbol": args.symbol,
        "broker": args.broker,
        "blueprint_id": args.blueprint,
        "start_ts": args.start_ts,
        "end_ts": args.end_ts,
        "params": params
    }

    if args.macro:
        payload["params"]["macro_id"] = args.macro
    
    print(f"Submitting backtest job {job_id} for {args.symbol} (Blueprint: {args.blueprint})...")
    await nc.publish("bt.request", json.dumps(payload).encode())
    
    done_event = asyncio.Event()

    async def message_handler(msg):
        data = json.loads(msg.data.decode())
        if data.get("job_id") == job_id:
            status = data.get("status")
            if status == "error":
                print("\n" + "!"*50)
                print(f"BACKTEST FAILED: {data.get('error')}")
                print("!"*50)
                done_event.set()
                return

            print("\n" + "="*50)
            print(f"BACKTEST COMPLETED: {job_id}")
            print("="*50)
            metrics = data.get("metrics", {})
            for k, v in metrics.items():
                print(f"{k.replace('_', ' ').title()}: {v}")
            print("="*50)
            print(f"Report available at: {metrics.get('report_url')}")
            print("="*50)
            done_event.set()

    sub = await nc.subscribe("bt.result", cb=message_handler)

    try:
        await done_event.wait()
    except KeyboardInterrupt:
        print("\nDisconnected.")
    finally:
        await sub.unsubscribe()
        await nc.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trigger a V3 Backtest")
    parser.add_argument("--symbol", type=str, default="BTC-USD")
    parser.add_argument("--broker", type=str, default="yfinance")
    parser.add_argument("--blueprint", type=str, default="213", help="213=Turtle, 19=Pairs")
    parser.add_argument("--start_ts", type=str, default="2024-06-01")
    parser.add_argument("--end_ts", type=str, default="2026-03-01")
    
    # Parámetros por compatibilidad
    parser.add_argument("--entry", type=int, default=20)
    parser.add_argument("--exit", type=int, default=10)
    parser.add_argument("--comm", type=float, default=0.0005)
    parser.add_argument("--slip", type=float, default=0.0)
    
    # Parámetros avanzados V3 (JSON)
    parser.add_argument("--params", type=str, help='JSON string: {"symbol_b": "ETH", "z_entry": 2.5}')
    parser.add_argument("--macro", type=str, help="FRED series ID for regime filtering (e.g. FEDFUNDS)")
    
    args = parser.parse_args()
    
    try:
        asyncio.run(trigger_backtest(args))
    except KeyboardInterrupt:
        pass
