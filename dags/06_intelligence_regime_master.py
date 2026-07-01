from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import sys

# Path to scripts
airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
scripts_dir = os.path.join(airflow_home, 'scripts')
intelligence_dir = os.path.join(scripts_dir, 'intelligence') # Assuming where intelligence scripts will be

default_args = {
    'owner': 'platform',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
}

with DAG(
    '06_intelligence_regime_master',
    default_args=default_args,
    description='Master Global Regime Detector (HMM)',
    schedule_interval='@daily',
    catchup=False,
    tags=['intelligence', 'macro', 'ml'],
) as dag:

    # 1. Update Macro Signals
    macro_symbols = [
        ('^VIX', 'yfinance'),
        ('SPY', 'yfinance'),
        ('HG=F', 'yfinance'),
        ('GC=F', 'yfinance'),
        ('T10Y2Y', 'fred')
    ]
    
    ingest_tasks = []
    for symbol, source in macro_symbols:
        safe_name = symbol.replace('^', '').replace('=', '_').replace('-', '_')
        t = BashOperator(
            task_id=f'ingest_{safe_name}',
            bash_command=f'python3 {scripts_dir}/data_loader_v3.py --symbol {symbol} --source {source} --period 2y --interval 1d'
        )
        ingest_tasks.append(t)

    # 2. Run HMM Analysis
    # We use BashOperator to run the script to ensure environment isolation/packages
    analyze_regime = BashOperator(
        task_id='analyze_market_regime',
        bash_command=f'python3 {scripts_dir}/intelligence/regime_master.py'
    )

    # Dependencies
    ingest_tasks >> analyze_regime
