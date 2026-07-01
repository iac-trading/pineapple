"""
=============================================================================
DAG: 11_data_funding_ingestion
=============================================================================
Ingesta institucional de historial de Funding Rates desde Binance Futures (USDT-M).
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
    logging.getLogger("FundingIngestion").warning(f"ccxt module missing: {e}")
    ccxt = None

DB_CONN = 'TRADING_DB'
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("FundingIngestion")

default_args = {
    "owner": "Senior Quant",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1), # History start
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def ingest_funding_rates(symbol: str, since_timestamp: int = None, **context):
    """
    Descarga el histórico de Funding Rates para un contrato lineal en Binance y 
    lo inserta en `market_funding`.
    """
    if not ccxt:
        raise ImportError("ccxt is not installed in the Airflow environment")

    exchange = ccxt.binance({
        'enableRateLimit': True,
        'options': {'defaultType': 'future'} # Must be future for funding
    })

    logger.info(f"Descargando Funding Rates históricos para {symbol}...")
    
    try:
        # Binance returns funding rates in pages, we need to paginate if necessary. 
        # CCXT handles basic fetch_funding_rate_history.
        all_funding = []
        limit = 1000
        current_since = since_timestamp or exchange.parse8601('2024-01-01T00:00:00Z')
        
        # Pull up to 5000 records max per run to prevent timeout
        for _ in range(5): 
            logger.info(f"Fetching from {exchange.iso8601(current_since)}...")
            rates = exchange.fetch_funding_rate_history(symbol, since=current_since, limit=limit)
            if not rates:
                break
                
            all_funding.extend(rates)
            
            # Update since for next pagination (last timestamp + 1ms)
            last_ts = rates[-1]['timestamp']
            if last_ts == current_since:
                break
            current_since = last_ts + 1
            
            # If we fetched less than the limit, we're caught up
            if len(rates) < limit:
                break

        if not all_funding:
            logger.info(f"No hay nuevos Funding Rates para {symbol}.")
            return

        # 2. Convertir a DataFrame y Limpiar
        df = pd.DataFrame(all_funding)
        df['ts'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        
        # Deduplicar por si acaso CCXT devolvió solapamientos
        df.drop_duplicates(subset=['timestamp'], inplace=True)
        
        records = []
        for _, row in df.iterrows():
            records.append((
                row['ts'],
                'binance',
                symbol.replace('/', ''), # e.g. BTCUSDT instead of BTC/USDT
                float(row['fundingRate']),
                '{"source": "ccxt_funding_history"}'
            ))

        # 3. Inserción Masiva
        pg_hook = PostgresHook(postgres_conn_id=DB_CONN)
        query = """
            INSERT INTO market_funding (ts, broker, symbol, rate, meta)
            VALUES %s
            ON CONFLICT (ts, broker, symbol) DO NOTHING;
        """

        from psycopg2.extras import execute_values
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        try:
            execute_values(cur, query, records)
            conn.commit()
            logger.info(f"Éxito: {len(records)} registros insertados en market_funding para {symbol}.")
        except Exception as e:
            conn.rollback()
            logger.error(f"Error en Bulk Insert: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    except Exception as e:
        logger.error(f"Falla crítica en la ingesta de funding: {e}")
        raise

with DAG(
    dag_id="11_data_funding_ingestion",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["institutional", "ingestion", "binance", "funding"],
) as dag:

    # Tarea para BTC/USDT por defecto
    ingest_btc_funding = PythonOperator(
        task_id="ingest_btc_funding",
        python_callable=ingest_funding_rates,
        op_kwargs={
            "symbol": "BTC/USDT:USDT", # CCXT convention for linear perps
        }
    )
    
    ingest_eth_funding = PythonOperator(
        task_id="ingest_eth_funding",
        python_callable=ingest_funding_rates,
        op_kwargs={
            "symbol": "ETH/USDT:USDT", 
        }
    )

    ingest_btc_funding >> ingest_eth_funding
