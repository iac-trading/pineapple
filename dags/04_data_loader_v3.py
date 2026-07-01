from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Add scripts directory to path (relative to this DAG file)
scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts")
sys.path.append(scripts_path)

try:
    from data_loader_v3 import MarketDataManager
except ImportError as e:
    print(f"Failed to import MarketDataManager: {e}")
    # Fallback to avoid breaking DAG parsing entirely if only the import fails
    class MarketDataManager:
        def load_data(self, *args, **kwargs):
            raise ImportError(f"MarketDataManager could not be imported: {e}")

default_args = {
    'owner': 'platform',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
}

def load_data_wrapper(symbol, source, period, interval):
    manager = MarketDataManager()
    manager.load_data(symbol, source, period, interval)

with DAG(
    '04_data_loader_backtest_v3',
    default_args=default_args,
    description='Bulk ingest financial data for Backtesting V3',
    schedule_interval='@weekly',
    catchup=False,
    tags=['v3', 'ingestion'],
) as dag:

    # Define tasks for symbols requested by user
    symbols = ['BTC/USDT', 'GC=F', 'SPY', 'QQQ'] # GC=F is Gold futures on yfinance
    
    for symbol in symbols:
        safe_name = symbol.replace('-', '_').replace('=', '_').replace('/', '_')
        source_api = 'binance' if 'BTC' in symbol else 'yfinance'
        PythonOperator(
            task_id=f'ingest_{safe_name}',
            python_callable=load_data_wrapper,
            op_kwargs={
                'symbol': symbol,
                'source': source_api,
                'period': '729d',
                'interval': '1h'
            },
        )

