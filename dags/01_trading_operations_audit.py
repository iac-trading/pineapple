"""
=============================================================================
DAG: 01_trading_operations_audit
=============================================================================
High-level overview of trading performance, exposure, and risk.
Targets business/operational metrics rather than infrastructure.
=============================================================================
"""

from __future__ import annotations
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

DB_CONN = "TRADING_DB"

default_args = {
    "owner": "Arquitecto",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def audit_portfolio_exposure(**context):
    """
    Summarizes net positions and basic PnL from v_strategy_performance.
    """
    hook = PostgresHook(postgres_conn_id=DB_CONN)
    
    # 1. Total Factory Exposure
    query = """
    SELECT 
        symbol, 
        SUM(net_position_qty) as total_qty,
        SUM(net_cashflow) as total_pnl_cash
    FROM v_strategy_performance
    WHERE status = 'running'
    GROUP BY symbol
    HAVING SUM(net_position_qty) != 0 OR SUM(net_cashflow) != 0;
    """
    rows = hook.get_records(query)
    
    summary = "\n" + "="*50 + "\n"
    summary += "      AXIO-QUANT PORTFOLIO EXPOSURE        \n"
    summary += "="*50 + "\n"
    summary += f"{'Symbol':<12} | {'Net Qty':<10} | {'Net PnL ($)':<12}\n"
    summary += "-"*40 + "\n"
    
    total_pnl = 0
    if not rows:
        summary += "  No open positions or major cashflows found.\n"
    for sym, qty, pnl in rows:
        summary += f"{sym:<12} | {qty:<10.2f} | {pnl:<12.2f}\n"
        total_pnl += (pnl or 0)
    
    summary += "="*50 + "\n"
    summary += f"  ESTIMATED REALIZED PnL: ${total_pnl:,.2f}\n"
    summary += "="*50 + "\n"
    
    print(summary)
    return summary

def audit_active_strategies(**context):
    """
    Checks for heartbeat and consistency of running strategies.
    """
    hook = PostgresHook(postgres_conn_id=DB_CONN)
    query = """
    SELECT name, symbol, broker, last_heartbeat 
    FROM strategy_instances 
    WHERE status = 'running' AND is_active = TRUE;
    """
    rows = hook.get_records(query)
    
    summary = "\n--- ACTIVE STRATEGIES HEARTBEAT ---\n"
    now = datetime.now(timezone.utc)
    
    for name, sym, broker, hb in rows:
        if hb:
            diff = (now - hb).total_seconds()
            status = "🟢 ON" if diff < 300 else "🔴 STALE"
            summary += f"  [{status}] {name:<20} ({sym}) | Last HB: {hb.strftime('%H:%M:%S')} UTC\n"
        else:
            summary += f"  [⚪ N/A] {name:<20} ({sym}) | No heartbeat record\n"
            
    print(summary)
    return summary

def audit_trade_volume(**context):
    """
    Counts recent filled orders to show operational 'advance'.
    """
    hook = PostgresHook(postgres_conn_id=DB_CONN)
    
    # Volume in different timeframes
    query = """
    SELECT 
        COUNT(*) FILTER (WHERE ts > NOW() - INTERVAL '1 hour') as last_1h,
        COUNT(*) FILTER (WHERE ts > NOW() - INTERVAL '24 hours') as last_24h,
        COUNT(*) FILTER (WHERE ts > NOW() - INTERVAL '7 days') as last_7d
    FROM orders 
    WHERE status = 'filled';
    """
    res = hook.get_first(query)
    
    summary = "\n--- OPERATIONAL ACTIVITY (Trades Filled) ---\n"
    summary += f"  Last 1h:  {res[0]:>5}\n"
    summary += f"  Last 24h: {res[1]:>5}\n"
    summary += f"  Last 7d:  {res[2]:>5}\n"
    
    print(summary)
    return summary

def ops_summary(**context):
    """
    Final operational status report.
    """
    ti = context['ti']
    portfolio = ti.xcom_pull(task_ids='audit_portfolio_exposure')
    strategies = ti.xcom_pull(task_ids='audit_active_strategies')
    volume = ti.xcom_pull(task_ids='audit_trade_volume')
    
    print("\n" + "#"*60)
    print("      AXIO-QUANT OPERATIONAL COMMAND CENTER")
    print("#"*60)
    print(portfolio)
    print(volume)
    print(strategies)
    print("\n" + "#"*60)

with DAG(
    dag_id="01_trading_operations_audit",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
    tags=["ops", "trading", "risk", "v3", "master"],
) as dag:

    # 1. Performance & Exposure
    t_exposure = PythonOperator(
        task_id="audit_portfolio_exposure",
        python_callable=audit_portfolio_exposure
    )

    # 2. Operational Volume
    t_volume = PythonOperator(
        task_id="audit_trade_volume",
        python_callable=audit_trade_volume
    )

    # 3. Strategy Health (Operational)
    t_strategies = PythonOperator(
        task_id="audit_active_strategies",
        python_callable=audit_active_strategies
    )

    # 4. Final Summary
    t_summary = PythonOperator(
        task_id="ops_summary",
        python_callable=ops_summary,
        trigger_rule=TriggerRule.ALL_DONE
    )

    t_exposure >> t_volume >> t_strategies >> t_summary
