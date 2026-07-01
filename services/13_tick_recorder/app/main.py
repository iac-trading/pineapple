"""
tick_recorder — Persiste ticks de NATS en market_ticks (TimescaleDB)

Suscribe a: md.<broker>.<symbol>.tick
Inserta en: market_ticks (broker, symbol, ts, bid, ask, last, meta)

Variables de entorno:
  NATS_URL              nats://user:pass@host:4222
  NATS_SUBJECTS         md.deriv.R_75.tick,md.deriv.R_50.tick  (separados por coma)
  POSTGRES_HOST/PORT/DB/USER/PASSWORD
  BATCH_SIZE            filas por INSERT (default 50)
  FLUSH_INTERVAL_SEC    segundos entre flushes (default 2)
  LOG_LEVEL             INFO
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
from collections import deque
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from nats.aio.client import Client as NATS

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] tick_recorder: %(message)s",
)
logger = logging.getLogger("TickRecorder")

NATS_URL          = os.getenv("NATS_URL", "nats://nats:4222")
NATS_SUBJECTS_RAW = os.getenv("NATS_SUBJECTS", "md.deriv.R_75.tick")
BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "50"))
FLUSH_INTERVAL    = float(os.getenv("FLUSH_INTERVAL_SEC", "2"))

DB = dict(
    host     = os.getenv("POSTGRES_HOST"),
    port     = int(os.getenv("POSTGRES_PORT", "5432")),
    dbname   = os.getenv("POSTGRES_DB", "trading"),
    user     = os.getenv("POSTGRES_USER", "tsdb"),
    password = os.getenv("POSTGRES_PASSWORD"),
    connect_timeout = 5,
)

# Buffer en memoria — deque es thread-safe para append/popleft
_buffer: deque[tuple] = deque()
_shutdown = False


def _db_conn():
    if not DB["host"] or not DB["password"]:
        raise RuntimeError("Faltan variables DB: POSTGRES_HOST / POSTGRES_PASSWORD")
    return psycopg2.connect(**DB)


def _flush(conn) -> int:
    """Inserta el buffer actual en market_ticks. Retorna filas insertadas."""
    if not _buffer:
        return 0

    batch = []
    while _buffer:
        batch.append(_buffer.popleft())

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO market_ticks (ts, broker, symbol, bid, ask, last, meta)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            batch,
            template="(%s, %s, %s, %s, %s, %s, %s::jsonb)",
        )
        conn.commit()

    return len(batch)


async def on_tick(msg):
    """Callback NATS — parsea el tick y lo agrega al buffer."""
    try:
        data = json.loads(msg.data.decode())
    except Exception:
        logger.warning("Tick no parseable subject=%s", msg.subject)
        return

    ts_raw = data.get("ts")
    broker = data.get("broker", "deriv")
    symbol = data.get("symbol", "")
    bid    = data.get("bid")
    ask    = data.get("ask")
    last   = data.get("last")
    meta   = json.dumps(data.get("meta") or {})

    # Normalizar timestamp
    try:
        if isinstance(ts_raw, (int, float)):
            ts = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
        else:
            ts = datetime.fromisoformat(str(ts_raw))
    except Exception:
        ts = datetime.now(timezone.utc)

    _buffer.append((ts, broker, symbol, bid, ask, last, meta))

    # Flush inmediato si el buffer supera el batch size
    if len(_buffer) >= BATCH_SIZE:
        logger.debug("Buffer lleno (%d), flush anticipado", len(_buffer))


async def flush_loop():
    """Flush periódico del buffer a la DB."""
    global _shutdown
    conn = None

    while not _shutdown:
        await asyncio.sleep(FLUSH_INTERVAL)

        if not _buffer:
            continue

        try:
            if conn is None or conn.closed:
                conn = _db_conn()

            inserted = _flush(conn)
            if inserted:
                logger.info("Persistidos %d ticks en market_ticks", inserted)

        except Exception as e:
            logger.error("Error flush DB: %s", e)
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None

    # Flush final al cerrar
    if conn and not conn.closed and _buffer:
        try:
            inserted = _flush(conn)
            logger.info("Flush final: %d ticks", inserted)
        except Exception as e:
            logger.error("Error en flush final: %s", e)


async def main():
    global _shutdown

    subjects = [s.strip() for s in NATS_SUBJECTS_RAW.split(",") if s.strip()]
    logger.info("Iniciando tick_recorder — subjects=%s batch=%d flush=%.1fs",
                subjects, BATCH_SIZE, FLUSH_INTERVAL)

    nc = NATS()
    await nc.connect(servers=[NATS_URL])

    for subject in subjects:
        await nc.subscribe(subject, cb=on_tick)
        logger.info("Suscrito a %s", subject)

    # Iniciar loop de flush en background
    flush_task = asyncio.create_task(flush_loop())

    # Graceful shutdown
    loop = asyncio.get_running_loop()

    def _handle_signal():
        global _shutdown
        logger.info("Señal de cierre recibida")
        _shutdown = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    logger.info("tick_recorder online, escuchando ticks...")

    while not _shutdown:
        await asyncio.sleep(1)

    await flush_task
    await nc.drain()
    logger.info("tick_recorder cerrado limpiamente")


if __name__ == "__main__":
    asyncio.run(main())
