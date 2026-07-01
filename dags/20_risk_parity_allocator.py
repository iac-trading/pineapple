"""
=============================================================================
DAG: 20_risk_parity_allocator
=============================================================================
Dynamic Capital Allocation for Axio-Quant Factory.
Manages a $10,000 virtual account using Risk Parity (Inverse Volatility).
=============================================================================
"""

import logging
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DB_CONN = "TRADING_DB"
TOTAL_CAPITAL_USD = 10000.0

default_args = {
    "owner": "Arquitecto",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def get_volatility(symbol, hook):
    """Calcula la volatilidad (StdDev de log-returns) de los últimos 1000 ticks."""
    sql = f"""
        SELECT last 
        FROM market_ticks 
        WHERE symbol = '{symbol}' 
        ORDER BY ts DESC 
        LIMIT 1000
    """
    df = hook.get_pandas_df(sql)
    if df.empty or len(df) < 50:
        logging.warning(f"Insuficientes datos para {symbol}. Usando volatilidad por defecto.")
        return 0.02 # 2% default
    
    # Calcular retornos logarítmicos
    prices = df['last'].values[::-1] # Invertir para que sea cronológico
    returns = np.diff(np.log(prices))
    vol = np.std(returns)
    return max(vol, 0.0001) # Evitar división por cero

def get_current_price(symbol, hook):
    sql = f"SELECT last FROM market_ticks WHERE symbol = '{symbol}' ORDER BY ts DESC LIMIT 1"
    res = hook.get_first(sql)
    return res[0] if res else 1.0

def run_risk_parity_allocation(**context):
    hook = PostgresHook(postgres_conn_id=DB_CONN)
    
    # 1. Obtener estrategias activas
    strategies_sql = """
        SELECT instance_id, symbol, name 
        FROM strategy_instances 
        WHERE is_active = TRUE AND desired_status = 'running'
    """
    df_strats = hook.get_pandas_df(strategies_sql)
    if df_strats.empty:
        logging.info("No hay estrategias activas para balancear.")
        return

    # 2. Calcular Volatilidad e Inverse Volatility para cada una
    strat_data = []
    inv_vol_sum = 0
    
    for _, row in df_strats.iterrows():
        vol = get_volatility(row['symbol'], hook)
        inv_vol = 1.0 / vol
        inv_vol_sum += inv_vol
        strat_data.append({
            'instance_id': row['instance_id'],
            'symbol': row['symbol'],
            'name': row['name'],
            'inv_vol': inv_vol
        })

    # 3. Calcular Pesos y Nuevas Qty
    allocation_results = []
    for s in strat_data:
        weight = s['inv_vol'] / inv_vol_sum
        target_usd = weight * TOTAL_CAPITAL_USD
        price = get_current_price(s['symbol'], hook)
        
        # Nueva cantidad (Qty)
        new_qty = target_usd / price
        
        # Redondear según el activo (BTC suele ser 4-5 decimales, R_75 puede ser diferente)
        # Por simplicidad usamos 4 decimales
        new_qty = round(new_qty, 4)
        
        allocation_results.append((new_qty, s['instance_id']))
        logging.info(f"STRAT: {s['name']} | Weight: {weight:.2%} | Target USD: ${target_usd:.2f} | New Qty: {new_qty}")

    # 4. Actualizar Base de Datos y Journal
    for qty, inst_id in allocation_results:
        # Update Strategy Qty
        hook.run(f"UPDATE strategy_instances SET qty = {qty} WHERE instance_id = '{inst_id}'")
        
        # Log event in Journal
        log_payload = json.dumps({
            "event": "capital_allocation",
            "method": "risk_parity",
            "new_qty": qty,
            "capital_usd": TOTAL_CAPITAL_USD
        })
        hook.run(f"""
            INSERT INTO journal_events (instance_id, event_type, actor, payload)
            VALUES ('{inst_id}', 'ALLOCATION_UPDATE', 'AirflowAllocator', '{log_payload}'::jsonb)
        """)

    logging.info("✅ Rebalanceo de cartera completado exitosamente.")

with DAG(
    dag_id="20_risk_parity_allocator",
    default_args=default_args,
    schedule_interval="@hourly", # Rebalancear cada hora
    catchup=False,
    tags=["growth", "risk", "capital"],
) as dag:

    allocate_task = PythonOperator(
        task_id="risk_parity_rebalance",
        python_callable=run_risk_parity_allocation,
    )
