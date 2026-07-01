from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
import logging
import sys

# Sincronización de rutas
sys.path.append('/opt/airflow/services/22_intelligence')

def run_commodity_analysis(**kwargs):
    from commodity_roll_analyzer import CommodityRollYieldAnalyzer
    
    # Podemos iterar por varios commodities si fuera necesario
    for symbol in ["CL=F", "GC=F"]:
        analyzer = CommodityRollYieldAnalyzer(symbol=symbol)
        result = analyzer.calculate_signal()
        
        if result:
            hook = PostgresHook(postgres_conn_id='TRADING_DB')
            conn = hook.get_conn()
            cur = conn.cursor()
            
            # Persistir señal
            cur.execute(
                """
                INSERT INTO intelligence_signals (ts, symbol, ofi, sentiment, vpin, metadata)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    result["ts"], 
                    f"ROLL_{symbol.replace('=F', '')}", 
                    result["roll_yield"], 
                    result["signal"], 
                    0.0, 
                    json.dumps(result)
                )
            )
            conn.commit()
            cur.close()
            conn.close()
            logging.info(f"Commodity Signal Persisted for {symbol}: {result['action']}")
        else:
            logging.warning(f"Data not available for {symbol} roll analysis.")

default_args = {
    'owner': 'axio-quant',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '16_alpha_commodity_roll',
    default_args=default_args,
    description='Estrategia 16: Captura de Roll Yield en Commodities (Oil/Gold)',
    schedule_interval='@daily',
    catchup=False,
    tags=['alpha', 'reactive', 'commodities']
) as dag:

    analyze_task = PythonOperator(
        task_id='analyze_commodity_roll_structure',
        python_callable=run_commodity_analysis,
    )
