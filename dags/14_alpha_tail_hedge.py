from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
import logging
import sys

# Sincronización de rutas para servicios compartidos
sys.path.append('/opt/airflow/services/22_intelligence')

def run_tail_hedge_analysis(**kwargs):
    from tail_risk_hedger import TailRiskHedger
    
    hedger = TailRiskHedger()
    spot = hedger.fetch_market_price("SPY")
    
    if spot:
        signal = hedger.calculate_hedge(spot)
        
        # Guardar la instrucción táctica en la base de datos
        hook = PostgresHook(postgres_conn_id='TRADING_DB')
        conn = hook.get_conn()
        cur = conn.cursor()
        
        # Usamos metadata para guardar los detalles de la opción
        cur.execute(
            """
            INSERT INTO intelligence_signals (ts, symbol, ofi, sentiment, vpin, metadata)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                signal["ts"], 
                "SPY_HEDGE", 
                signal["target_strike"], # Guardamos strike en OFI
                1.0,                    # Señal de compra siempre activa para el hedge
                0.0, 
                json.dumps(signal)
            )
        )
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Hedge instruction persisted for SPY at {signal['target_strike']}")
    else:
        logging.warning("Underlying price for SPY not found. Cannot calculate hedge.")

default_args = {
    'owner': 'axio-quant',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    '14_alpha_tail_hedge',
    default_args=default_args,
    description='Estrategia 20: Protección sistemática contra Cisnes Negros',
    schedule_interval='@weekly', # El seguro se revisa semanalmente
    catchup=False,
    tags=['alpha', 'reactive', 'risk']
) as dag:

    hedge_task = PythonOperator(
        task_id='calculate_tail_hedge',
        python_callable=run_tail_hedge_analysis,
    )
