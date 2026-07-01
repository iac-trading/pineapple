"""
=============================================================================
DAG: 02_binance_data_ingestion
=============================================================================
Ingesta institucional de datos OHLCV desde Binance.
Mapea Close -> last para compatibilidad con market_ticks.
=============================================================================
"""

import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    import ccxt
except ImportError as e:
    # Log it, but don't break the entire Airflow DAG parser
    logging.getLogger("BinanceIngestion").warning(f"ccxt module missing: {e}")
    ccxt = None


# Configuración básica
DB_CONN = 'TRADING_DB'
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("BinanceIngestion")

default_args = {
    "owner": "Senior Quant",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def ingest_binance_ohlcv(symbol: str, interval: str = '15m', limit: int = 1000, **context):
    """
    Descarga datos de Binance via CCXT, limpia con Pandas e inserta en TimescaleDB.
    """
    exchange = ccxt.binance({
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'}
    })

    logger.info(f"Descargando {limit} velas de {symbol} ({interval}) desde Binance...")
    
    try:
        # 1. Descarga (OHLCV: timestamp, open, high, low, close, volume)
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=interval, limit=limit)
        if not ohlcv:
            logger.warning("No se recibieron datos de Binance.")
            return

        # 2. Convertir a DataFrame y Limpiar
        df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
        df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
        
        # Mapeo institucional solicitado: Close -> last
        # Los campos bid/ask se envían como None/NaN para velas históricas
        records = []
        for _, row in df.iterrows():
            records.append((
                row['ts'],
                'binance',
                symbol.replace('/', ''),
                None, # bid
                None, # ask
                float(row['close']), # last (mapeado de Close)
                '{"source": "ccxt_ohlcv", "type": "institutional_ingest"}'
            ))

        # 3. Inserción Masiva
        pg_hook = PostgresHook(postgres_conn_id=DB_CONN)
        
        # query con ON CONFLICT para evitar duplicados
        # Se asume que ts y symbol son parte de la PK o UNIQUE index
        query = """
            INSERT INTO market_ticks (ts, broker, symbol, bid, ask, last, meta)
            VALUES %s
            ON CONFLICT (ts, broker, symbol) DO NOTHING;
        """

        from psycopg2.extras import execute_values
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        try:
            execute_values(cur, query, records)
            conn.commit()
            logger.info(f"Éxito: {len(records)} registros insertados en market_ticks.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error en Bulk Insert: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    except Exception as e:
        logger.error(f"Falla crítica en la ingesta: {e}")
        raise

with DAG(
    dag_id="02_binance_data_ingestion",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["institutional", "ingestion", "binance", "ohlcv"],
) as dag:

    # Tarea para BTC/USDT por defecto
    ingest_btc = PythonOperator(
        task_id="ingest_btc_usdt",
        python_callable=ingest_binance_ohlcv,
        op_kwargs={
            "symbol": "BTC/USDT",
            "interval": "15m",
            "limit": 1000
        }
    )

    ingest_btc
