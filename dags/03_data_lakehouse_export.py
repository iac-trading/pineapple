import pandas as pd
import psycopg2
import os
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator

DB_CONN_STR = os.getenv("POSTGRES_URL", "postgresql://platform:platform@192.168.100.201:5432/platform")
EXPORT_PATH = "/opt/platform/datalake"

default_args = {
    "owner": "Arquitecto",
    "start_date": datetime(2026, 3, 1),
    "retries": 1,
}

def export_to_parquet():
    # Use PostgresHook to automatically get the connection string from Airflow
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    os.makedirs(EXPORT_PATH, exist_ok=True)
    hook = PostgresHook(postgres_conn_id="TRADING_DB")
    conn = hook.get_conn()
    
    # Export Ticks
    print("Exporting market_ticks...")
    try:
        df_ticks = pd.read_sql("SELECT * FROM market_ticks WHERE ts >= NOW() - INTERVAL '24 hours'", conn)
        if not df_ticks.empty:
            fname = f"{EXPORT_PATH}/ticks_{datetime.now().strftime('%Y%m%d_%H')}.parquet"
            df_ticks.to_parquet(fname, index=False)
            print(f"Saved {len(df_ticks)} ticks to {fname}")
    except Exception as e:
        print(f"Warning: Could not export market_ticks: {e}")

    # Export Candles (if table exists)
    print("Exporting market_candles...")
    try:
        df_candles = pd.read_sql("SELECT * FROM market_candles WHERE ts >= NOW() - INTERVAL '24 hours'", conn)
        if not df_candles.empty:
            fname = f"{EXPORT_PATH}/candles_{datetime.now().strftime('%Y%m%d_%H')}.parquet"
            df_candles.to_parquet(fname, index=False)
            print(f"Saved {len(df_candles)} candles to {fname}")
    except Exception as e:
        print(f"Warning: Could not export market_candles: {e}")
        
    conn.close()

with DAG(
    "03_data_lakehouse_export",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["data", "analytics", "parquet"],
) as dag:

    export_task = PythonOperator(
        task_id="export_daily_data",
        python_callable=export_to_parquet
    )
