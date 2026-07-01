"""
=============================================================================
DAG: 99_journal_cleanup
=============================================================================
Utility DAG for cleaning the trading journal and orders tables.
Can be triggered manually from Airflow with optional symbol filter.
=============================================================================
"""

from __future__ import annotations
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param

DB_CONN = "TRADING_DB"

default_args = {
    "owner": "Arquitecto",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "retries": 0,
}

def clean_orders(**context):
    """
    Borra registros de la tabla 'orders'.
    Si se pasa 'symbol' en params, solo borra ese símbolo.
    Si se pasa 'older_than_days', solo borra registros más viejos que N días.
    Si ambos están vacíos, borra TODOS los registros (TRUNCATE).
    """
    hook = PostgresHook(postgres_conn_id=DB_CONN)
    params = context.get("params", {})
    symbol = params.get("symbol", "").strip()
    older_than_days = params.get("older_than_days", 0)

    if symbol and older_than_days:
        sql = f"DELETE FROM orders WHERE symbol = %s AND ts < NOW() - INTERVAL '{int(older_than_days)} days';"
        hook.run(sql, parameters=(symbol,))
        logging.info(f"✅ Orders deleted: symbol={symbol}, older_than={older_than_days}d")
    elif symbol:
        sql = "DELETE FROM orders WHERE symbol = %s;"
        hook.run(sql, parameters=(symbol,))
        logging.info(f"✅ Orders deleted for symbol: {symbol}")
    elif older_than_days:
        sql = f"DELETE FROM orders WHERE ts < NOW() - INTERVAL '{int(older_than_days)} days';"
        hook.run(sql)
        logging.info(f"✅ Orders older than {older_than_days} days deleted.")
    else:
        hook.run("TRUNCATE TABLE orders;")
        logging.info("✅ All orders TRUNCATED.")


def clean_journal_events(**context):
    """
    Borra registros de la tabla 'journal_events'.
    Misma lógica de filtros que clean_orders.
    """
    hook = PostgresHook(postgres_conn_id=DB_CONN)
    params = context.get("params", {})
    symbol = params.get("symbol", "").strip()
    older_than_days = params.get("older_than_days", 0)

    if symbol and older_than_days:
        sql = f"DELETE FROM journal_events WHERE payload->>'symbol' = %s AND ts < NOW() - INTERVAL '{int(older_than_days)} days';"
        hook.run(sql, parameters=(symbol,))
        logging.info(f"✅ Journal events deleted: symbol={symbol}, older_than={older_than_days}d")
    elif symbol:
        sql = "DELETE FROM journal_events WHERE payload->>'symbol' = %s;"
        hook.run(sql, parameters=(symbol,))
        logging.info(f"✅ Journal events deleted for symbol: {symbol}")
    elif older_than_days:
        sql = f"DELETE FROM journal_events WHERE ts < NOW() - INTERVAL '{int(older_than_days)} days';"
        hook.run(sql)
        logging.info(f"✅ Journal events older than {older_than_days} days deleted.")
    else:
        hook.run("TRUNCATE TABLE journal_events;")
        logging.info("✅ All journal events TRUNCATED.")


def report_counts(**context):
    """
    Muestra cuántos registros quedan después de la limpieza.
    """
    hook = PostgresHook(postgres_conn_id=DB_CONN)
    orders_count = hook.get_first("SELECT COUNT(*) FROM orders;")[0]
    journal_count = hook.get_first("SELECT COUNT(*) FROM journal_events;")[0]

    summary = (
        "\n" + "=" * 50 + "\n"
        "  POST-CLEANUP RECORD COUNTS\n"
        "=" * 50 + "\n"
        f"  orders:         {orders_count:>8} rows\n"
        f"  journal_events: {journal_count:>8} rows\n"
        + "=" * 50
    )
    print(summary)
    return summary


with DAG(
    dag_id="99_journal_cleanup",
    default_args=default_args,
    schedule_interval=None,   # Solo manual / on-demand
    catchup=False,
    tags=["ops", "maintenance", "journal"],
    params={
        "symbol": Param(
            default="",
            type="string",
            description="Opcional: símbolo a limpiar (e.g. 'R_10'). Vacío = todos.",
        ),
        "older_than_days": Param(
            default=0,
            type="integer",
            description="Opcional: solo borrar registros más viejos que N días. 0 = todos.",
        ),
    },
) as dag:

    t1 = PythonOperator(
        task_id="clean_orders",
        python_callable=clean_orders,
    )

    t2 = PythonOperator(
        task_id="clean_journal_events",
        python_callable=clean_journal_events,
    )

    t3 = PythonOperator(
        task_id="report_counts",
        python_callable=report_counts,
    )

    t1 >> t2 >> t3
