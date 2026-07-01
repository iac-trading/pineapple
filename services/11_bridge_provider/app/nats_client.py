import json
from nats.aio.client import Client as NATS

class NatsBus:
    def __init__(self, url: str):
        self.url = url
        self.nc = NATS()

    async def connect(self):
        await self.nc.connect(servers=[self.url])

    async def publish_json(self, subject: str, payload: dict):
        await self.nc.publish(subject, json.dumps(payload).encode())

    async def subscribe(self, subject: str, cb):
        await self.nc.subscribe(subject, cb=cb)

    async def close(self):
        if self.nc.is_connected:
            await self.nc.drain()
