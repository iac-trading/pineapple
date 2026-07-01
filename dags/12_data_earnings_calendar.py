"""
=============================================================================
DAG: 12_data_earnings_calendar
=============================================================================
Scrapes upcoming and historical corporate earnings dates for the IV Crush
strategy (BP-64). It populates the `corporate_events` table in TimescaleDB.
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
    import yfinance as yf
except ImportError as e:
    logging.getLogger("EarningsIngestion").warning(f"yfinance module missing: {e}")
    yf = None

# Configuración básica
DB_CONN = 'TRADING_DB'
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("EarningsIngestion")

default_args = {
    "owner": "Senior Quant",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def fetch_and_store_earnings(ticker: str, **context):
    """
    Fetches earnings history and upcoming dates for a specific ticker using yfinance,
    and upserts the records into the corporate_events table.
    """
    if yf is None:
        logger.error("yfinance library is not installed. Skipping task.")
        return

    logger.info(f"Fetching Earnings Calendar for {ticker} using yfinance...")
    
    try:
        tk = yf.Ticker(ticker)
        # yfinance returns a DataFrame where index is the Datetime of the earning
        earnings_df = tk.earnings_dates
        
        if earnings_df is None or earnings_df.empty:
            logger.warning(f"No earnings history found for {ticker} by yfinance")
            return
            
        # Reset index to access the Timestamp
        df = earnings_df.reset_index()
        # The column is called 'Earnings Date'
        date_col = 'Earnings Date'
        if date_col not in df.columns:
            logger.error(f"'{date_col}' not found in yfinance DataFrame for {ticker}")
            return
            
        records = []
        for _, row in df.iterrows():
            evt_datetime = row[date_col]
            if pd.isna(evt_datetime):
                continue
                
            event_date = evt_datetime.date()
            
            # yfinance returns UTC timestamps. If hour is 0, usually implies unknown or BMO. 
            evt_time = "BMO" if evt_datetime.hour < 12 else "AMC"
            
            # Additional meta
            eps_est = row.get('EPS Estimate', None)
            eps_act = row.get('Reported EPS', None)
            
            meta = {
                "eps_estimate": eps_est if pd.notna(eps_est) else None,
                "eps_actual": eps_act if pd.notna(eps_act) else None,
                "source": "yfinance"
            }
            
            # Using str() for meta to cleanly insert as JSONB via PostgresHook
            import json
            records.append((
                ticker,
                event_date,
                'EARNINGS',
                evt_time,
                json.dumps(meta)
            ))
            
        if not records:
            logger.info(f"No valid earnings records to insert for {ticker}")
            return

        # Database Insertion
        hook = PostgresHook(postgres_conn_id=DB_CONN)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        # Upsert logic (ON CONFLICT DO UPDATE)
        insert_query = """
            INSERT INTO corporate_events (symbol, event_date, event_type, time_of_day, meta)
            VALUES %s
            ON CONFLICT (symbol, event_date, event_type) 
            DO UPDATE SET 
                time_of_day = EXCLUDED.time_of_day,
                meta = EXCLUDED.meta,
                updated_at = NOW();
        """
        
        from psycopg2.extras import execute_values
        execute_values(cur, insert_query, records)
        conn.commit()
        
        logger.info(f"Successfully upserted {len(records)} earnings events for {ticker} into corporate_events")
        
    except Exception as e:
        logger.error(f"Error processing earnings for {ticker}: {e}")
        raise

with DAG(
    dag_id="12_data_earnings_calendar",
    default_args=default_args,
    schedule_interval="0 1 * * 0",  # Run weekly on Sundays at 01:00
    catchup=False,
    max_active_runs=1,
    tags=["finance", "data", "corpactions", "iv_crush", "BP-64"],
) as dag:

    # Universe of tickers to track for IV Crush Strategy
    TARGET_TICKERS = ["AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "META", "GOOGL"]

    for ticker in TARGET_TICKERS:
        PythonOperator(
            task_id=f"fetch_earnings_{ticker}",
            python_callable=fetch_and_store_earnings,
            op_kwargs={"ticker": ticker},
        )
