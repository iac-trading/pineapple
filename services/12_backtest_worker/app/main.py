import os
import json
import time
import logging
import asyncio
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor
from nats.aio.client import Client as NATS
from dateutil import parser as dtparser

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] backtest_worker: %(message)s")
logger = logging.getLogger("BacktestWorker")

NATS_URL = os.getenv("NATS_URL", "nats://nats:4222")

DB = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "platform"),
    "user": os.getenv("POSTGRES_USER", "platform"),
    "password": os.getenv("POSTGRES_PASSWORD") or os.getenv("POSTGRES_PASS"),
    "connect_timeout": 5,
}

POLL_SECONDS = float(os.getenv("POLL_SECONDS", "2"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "25"))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def conn():
    if not DB["host"] or not DB["password"]:
        raise RuntimeError("Missing DB env: POSTGRES_HOST/POSTGRES_PASSWORD")
    return psycopg2.connect(**DB)


def claim_jobs(limit: int) -> list[dict]:
    with conn() as c, c.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT job_id, blueprint_id, instance_id, broker, symbol, start_ts, end_ts, params
            FROM backtest_jobs
            WHERE status='queued'
            ORDER BY created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
        if not rows:
            c.rollback()
            return []

        job_ids = [str(r["job_id"]) for r in rows]
        cur.execute(
            """
            UPDATE backtest_jobs
            SET status='running', updated_at=now(), error=NULL
            WHERE job_id = ANY(%s::uuid[])
            """,
            (job_ids,),
        )
        c.commit()
        return rows


async def publish_jobs(nc: NATS, jobs: list[dict]):
    for r in jobs:
        msg = {
            "schema": "backtest.request.v1",
            "job_id": str(r["job_id"]),
            "blueprint_id": r.get("blueprint_id"),
            "instance_id": str(r["instance_id"]) if r.get("instance_id") else None,
            "broker": r.get("broker") or "paper",
            "symbol": r.get("symbol"),
            "start_ts": r["start_ts"].isoformat() if r.get("start_ts") else None,
            "end_ts": r["end_ts"].isoformat() if r.get("end_ts") else None,
            "params": r.get("params") or {},
            "ts": utc_now_iso(),
        }
        await nc.publish("bt.request", json.dumps(msg).encode())


async def main():
    nc = NATS()
    await nc.connect(servers=[NATS_URL])
    logger.info("BacktestWorker online. Poll=%ss NATS=%s", POLL_SECONDS, NATS_URL)

    while True:
        try:
            jobs = claim_jobs(BATCH_SIZE)
            if jobs:
                await publish_jobs(nc, jobs)
                logger.info("Published %d bt.request jobs", len(jobs))
            else:
                await asyncio.sleep(POLL_SECONDS)
        except Exception as e:
            logger.exception("Loop error: %s", e)
            await asyncio.sleep(POLL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())