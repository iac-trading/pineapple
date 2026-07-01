"""
=============================================================================
DAG: 19_latency_auditor_burnin
=============================================================================
Auditoría de latencia institucional para el periodo de rodaje (Burn-in).
Consulta la vista v_execution_latency y reporta anomalías.
=============================================================================
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import os

DB_CONN = "TRADING_DB"

default_args = {
    "owner": "Arquitecto",
    "start_date": datetime(2026, 3, 17),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

def audit_latency():
    hook = PostgresHook(postgres_conn_id=DB_CONN)
    conn = hook.get_conn()
    cur = conn.cursor()
    
    # Consultar las últimas 50 operaciones para estadísticas de latencia
    cur.execute("""
        SELECT 
            AVG(latency_ms) as avg_lat,
            MAX(latency_ms) as max_lat,
            COUNT(*) as total_ops
        FROM v_execution_latency
        WHERE filled_at > NOW() - INTERVAL '24 hours';
    """)
    stats = cur.fetchone()
    
    print("="*50)
    print("INSTITUTIONAL LATENCY AUDIT (LAST 24H)")
    print("="*50)
    if stats and stats[2] > 0:
        avg_lat, max_lat, total_ops = stats
        print(f"Total Operations: {total_ops}")
        print(f"Average Latency:  {avg_lat:.2f} ms")
        print(f"Max Latency:      {max_lat:.2f} ms")
        
        # Alerta si la latencia promedio supera los 1000ms (ajustable)
        if avg_lat > 1000:
            print("\nWARNING: High latency detected in the execution pipeline!")
    else:
        print("No operations detected in the last 24 hours.")
    
    print("="*50)
    cur.close()
    conn.close()

with DAG(
    dag_id="19_latency_auditor_burnin",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
    tags=["burn-in", "phase19", "latency"],
) as dag:

    audit_task = PythonOperator(
        task_id="audit_latency_metrics",
        python_callable=audit_latency,
    )
