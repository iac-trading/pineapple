import os
import json
import asyncio
import argparse
from nats.aio.client import Client as NATS
from datetime import datetime

class ControlCLI:
    def __init__(self, nats_url):
        self.nc = NATS()
        self.nats_url = nats_url

    async def connect(self):
        await self.nc.connect(servers=[self.nats_url])
        print(f"Connected to NATS: {self.nats_url}")

    async def ping_all(self, timeout=2.5):
        print("Discovering active strategies...")
        reply_inbox = self.nc.new_inbox()
        responses = []
        
        async def on_response(msg):
            try:
                raw = msg.data.decode().strip()
                if raw:
                    responses.append(json.loads(raw))
            except Exception:
                pass
            
        sub = await self.nc.subscribe(reply_inbox, cb=on_response)
        await self.nc.flush() # Ensure subscription is registered
        await self.nc.publish("factory.control.ping", b"", reply=reply_inbox)
        
        await asyncio.sleep(timeout)
        await sub.unsubscribe()
        
        if not responses:
            print("No active strategies found.")
            return
            
        print("-" * 60)
        print(f"{'STRATEGY':<20} | {'SYMBOL':<10} | {'INSTANCE_ID':<30}")
        print("-" * 60)
        for resp in responses:
            s_name = str(resp.get('strategy', 'Unknown'))
            s_sym = str(resp.get('symbol', 'N/A'))
            s_id = str(resp.get('instance_id', 'Unknown'))
            print(f"{s_name:<20} | {s_sym:<10} | {s_id}")
        print("-" * 60)

    async def set_params(self, instance_id, params_json):
        subject = f"factory.control.params.{instance_id}"
        try:
            params = json.loads(params_json)
            await self.nc.publish(subject, json.dumps(params).encode())
            print(f"✅ Hot-Swap request sent to {subject}")
        except json.JSONDecodeError:
            print("❌ Error: Invalid JSON format for parameters.")

    async def orchestrate(self, action):
        # Action is 'pause' or 'resume'
        subject = "factory.orchestration"
        payload = {"action": action.upper(), "ts": datetime.now().isoformat(), "sender": "manual_cli"}
        await self.nc.publish(subject, json.dumps(payload).encode())
        print(f"✅ Orchestration signal sent: {action.upper()}")

async def main():
    parser = argparse.ArgumentParser(description="Axio-Quant Tactical Control CLI")
    parser.add_argument("--nats", default=os.environ["NATS_URL"], help="NATS URL")
    
    subparsers = parser.add_subparsers(dest="command")
    
    # Ping command
    subparsers.add_parser("ping", help="Discover active strategies")
    
    # Param command
    param_parser = subparsers.add_parser("param", help="Update strategy parameters (Hot-Swap)")
    param_parser.add_argument("--id", required=True, help="Instance ID of the strategy")
    param_parser.add_argument("--json", required=True, help='Params in JSON format, e.g. \'{"z_entry": 2.5}\'')
    
    # Orchestrate command
    orch_parser = subparsers.add_parser("orch", help="Global orchestration (pause/resume)")
    orch_parser.add_argument("action", choices=["pause", "resume"], help="Action to perform")
    
    args = parser.parse_args()
    
    cli = ControlCLI(args.nats)
    await cli.connect()
    
    if args.command == "ping":
        await cli.ping_all()
    elif args.command == "param":
        await cli.set_params(args.id, args.json)
    elif args.command == "orch":
        await cli.orchestrate(args.action)
    else:
        parser.print_help()
        
    await cli.nc.close()

if __name__ == "__main__":
    asyncio.run(main())
