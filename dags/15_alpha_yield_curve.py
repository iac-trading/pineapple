from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
import logging
import sys

# Sincronización de rutas
sys.path.append('/opt/airflow/services/22_intelligence')

def run_yield_analysis(**kwargs):
    from yield_curve_analyzer import YieldCurveButterflyAnalyzer
    
    analyzer = YieldCurveButterflyAnalyzer()
    result = analyzer.analyze_curvature()
    
    if result:
        hook = PostgresHook(postgres_conn_id='TRADING_DB')
        conn = hook.get_conn()
        cur = conn.cursor()
        
        # Persistir señal de curvatura
        cur.execute(
            """
            INSERT INTO intelligence_signals (ts, symbol, ofi, sentiment, vpin, metadata)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                result["ts"], 
                "YIELD_BTFY", 
                result["butterfly_val"], # Valor del butterfly en OFI
                result["signal"],        # Señal en sentiment
                result["zscore"],        # Z-Score en VPIN para auditoría
                json.dumps(result)
            )
        )
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Yield Signal Persisted: {result['action']} (Z: {result['zscore']})")
    else:
        logging.warning("Yield data not available for analysis.")

default_args = {
    'owner': 'axio-quant',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '15_alpha_yield_curve',
    default_args=default_args,
    description='Estrategia 12: Arbitraje de Curvatura de Tipos (Butterfly)',
    schedule_interval='@daily',
    catchup=False,
    tags=['alpha', 'reactive', 'rates']
) as dag:

    analyze_task = PythonOperator(
        task_id='analyze_yield_curve_convexity',
        python_callable=run_yield_analysis,
    )
