from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
import logging

# Importar el analizador desde el servicio de inteligencia
import sys
import os
sys.path.append('/opt/airflow/plugins') # Ruta típica en el contenedor para servicios compartidos
# Para el desarrollo local y sincronización vía Ansible:
sys.path.append('/opt/airflow/services/22_intelligence')

def run_vix_analysis(**kwargs):
    from vix_term_structure_analyzer import VixTermStructureAnalyzer
    
    analyzer = VixTermStructureAnalyzer()
    signal = analyzer.generate_signal()
    
    # Persistir la señal para que el monitor la vea
    if signal["status"] != "NO_DATA":
        hook = PostgresHook(postgres_conn_id='TRADING_DB')
        conn = hook.get_conn()
        cur = conn.cursor()
        
        cur.execute(
            """
            INSERT INTO intelligence_signals (ts, symbol, ofi, sentiment, vpin, metadata)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                signal["ts"], 
                "VIX_TERM", 
                signal["contango_pct"], # Guardamos contango en OFI para simplificar esquema
                signal["signal"],       # Guardamos señal en sentiment
                0.0, 
                json.dumps(signal)
            )
        )
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Signal persisted: {signal['action']}")
    else:
        logging.warning("No VIX data available for injection.")

default_args = {
    'owner': 'axio-quant',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '13_alpha_vix_structure',
    default_args=default_args,
    description='Estrategia 11: Captura de Roll Yield en Futuros VIX',
    schedule_interval='0 23 * * *', # Ejecutar al final del día
    catchup=False,
    tags=['alpha', 'reactive', 'vix']
) as dag:

    analyze_task = PythonOperator(
        task_id='analyze_vix_term_structure',
        python_callable=run_vix_analysis,
    )
