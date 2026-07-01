"""
=============================================================================
DAG: 02_axio_backtest_orchestrator
=============================================================================
Orquestador Universal de Backtesting para Axio-Quant.
Permite ejecutar cualquier blueprint (01-10, 101, 213, etc.) con parámetros
configurables desde la UI de Airflow.

Características:
  - Soporte para ejecución simple o Batch (Optimization Grid).
  - Integración con NATS y PostgreSQL (nodo .201).
  - Ranking institucional automático.
=============================================================================
"""

import itertools
import json
import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

DB_CONN = "TRADING_DB"

default_args = {
    "owner": "Senior-Quant",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

def _generate_jobs_logic(**context):
    conf = context.get("dag_run").conf or {}
    
    # Parámetros base
    symbol = conf.get("symbol", "BTC-USD")
    broker = conf.get("broker", "binance")
    start_ts = conf.get("start_ts", "2025-01-01")
    end_ts = conf.get("end_ts", "2026-03-01")
    blueprint_id = conf.get("blueprint_id", "06")
    
    # Manejo de Grid de Parámetros (Optimization)
    param_grid = conf.get("param_grid") # Opcional: {"fast_p": [5, 10], "slow_p": [20, 30]}
    
    jobs_to_create = []
    if param_grid:
        keys = list(param_grid.keys())
        values = list(param_grid.values())
        combos = [dict(zip(keys, combo)) for combo in itertools.product(*values)]
        for combo in combos:
            jobs_to_create.append(combo)
    else:
        # Fallback a parámetros simples
        jobs_to_create.append(conf.get("params", {}))

    hook = PostgresHook(postgres_conn_id=DB_CONN)
    conn = hook.get_conn()
    cur = conn.cursor()
    
    job_ids = []
    for params in jobs_to_create:
        job_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO backtest_jobs 
            (job_id, blueprint_id, broker, symbol, start_ts, end_ts, params, status)
            VALUES (%s::uuid, %s, %s, %s, %s::timestamptz, %s::timestamptz, %s::jsonb, 'queued')
            """,
            (job_id, blueprint_id, broker, symbol, start_ts, end_ts, json.dumps(params))
        )
        job_ids.append(job_id)
        
    conn.commit()
    cur.close()
    conn.close()
    
    context["ti"].xcom_push(key="job_ids", value=job_ids)
    print(f"✅ Se han encolado {len(job_ids)} variaciones de backtest para {blueprint_id}")

def _wait_and_rank_logic(**context):
    import time
    job_ids = context["ti"].xcom_pull(key="job_ids", task_ids="generate_jobs")
    if not job_ids: return
    
    hook = PostgresHook(postgres_conn_id=DB_CONN)
    total = len(job_ids)
    
    # Polling institucional
    start_time = time.time()
    while (time.time() - start_time) < 1800: # 30 min timeout
        cur = hook.get_conn().cursor()
        cur.execute(
            "SELECT count(*) FROM backtest_jobs WHERE job_id = ANY(%s::uuid[]) AND status IN ('done', 'error')",
            (job_ids,)
        )
        finished = cur.fetchone()[0]
        cur.close()
        
        if finished >= total: break
        print(f"Esperando resultados... {finished}/{total} completados")
        time.sleep(15)
        
    # Extraer Ranking
    cur = hook.get_conn().cursor()
    cur.execute(
        """
        SELECT 
            blueprint_id, 
            metrics->>'engine' as engine,
            (metrics->>'sharpe')::float as sharpe,
            (metrics->>'total_return')::float as ret,
            (metrics->>'max_drawdown')::float as dd,
            metrics->>'report_url' as url
        FROM backtest_results
        WHERE job_id = ANY(%s::uuid[])
        ORDER BY sharpe DESC
        """,
        (job_ids,)
    )
    results = cur.fetchall()
    cur.close()
    
    print("\n" + "="*80)
    print(f"📊 AXIO-QUANT RANKING - {blueprint_id}")
    print("="*80)
    print(f"{'Engine':<20} | {'Sharpe':^10} | {'Return':^10} | {'MaxDD':^10} | {'Report'}")
    print("-"*80)
    for r in results:
        print(f"{r[1]:<20} | {r[2]:^10.2f} | {r[3]:^10.2%} | {r[4]:^10.2%} | {r[5]}")
    print("="*80)

with DAG(
    "02_axio_backtest_orchestrator",
    default_args=default_args,
    description="Orquestador Maestro Institucional de Backtesting",
    schedule_interval=None,
    catchup=False,
    tags=["axio", "backtest", "orchestrator", "v3", "institutional"]
) as dag:

    generate_jobs = PythonOperator(
        task_id="generate_jobs",
        python_callable=_generate_jobs_logic
    )

    process_results = PythonOperator(
        task_id="wait_and_rank",
        python_callable=_wait_and_rank_logic
    )

    generate_jobs >> process_results
