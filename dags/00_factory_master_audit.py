"""
=============================================================================
DAG: 00_factory_master_audit
=============================================================================
Consolidated audit and health monitor for Axio-Quant Factory V3.
Validated both Infrastructure (SSH/Docker/External) and Strategy health.
=============================================================================
"""

from __future__ import annotations
import socket
import logging
import json
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

DB_CONN = "TRADING_DB"

default_args = {
    "owner": "Arquitecto",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# =============================================================================
# INFRA FUNCTIONS
# =============================================================================

def check_port(host, port, timeout=5, service_name=""):
    label = service_name or f"{host}:{port}"
    try:
        with socket.create_connection((host, port), timeout=timeout):
            print(f"OK {label} puerto {port} accesible")
            return True
    except Exception as e:
        print(f"FAIL {label} ({host}:{port}) -> {e}")
        return False

def check_internet(**ctx):    return check_port("8.8.8.8", 53, service_name="Internet (DNS)")
def check_nats_client(**ctx): return check_port("192.168.100.200", 4222, service_name="NATS Client")
def check_grafana(**ctx):     return check_port("192.168.100.200", 3000, service_name="Grafana")
def check_binance_api(**ctx): return check_port("api.binance.com", 443,  service_name="Binance API")
def check_ibkr_bridge(**ctx): return check_port("192.168.100.202", 4001, service_name="IBKR Gateway")
def check_kraken_api(**ctx):  return check_port("api.kraken.com", 443,  service_name="Kraken API")
def check_deriv_api(**ctx):   return check_port("api.deriv.com", 443,   service_name="Deriv API")

# =============================================================================
# STRATEGY HEALTH FUNCTIONS (Merged from 05_monitor)
# =============================================================================

def audit_strategy_health(**context):
    hook = PostgresHook(postgres_conn_id=DB_CONN)
    conn = hook.get_conn()
    cur = conn.cursor()
    
    # 1. Strategy Instances Summary
    cur.execute("""
        SELECT status, COUNT(*)
        FROM strategy_instances
        GROUP BY status
    """)
    rows = cur.fetchall()
    summary = "\n--- STRATEGY INSTANCES (Inventory) ---\n"
    summary += f"{'Status':<12} | {'Count':<6}\n"
    summary += "-" * 25 + "\n"
    for status, count in rows:
        summary += f"{str(status):<12} | {count:<6}\n"

    # 2. Data Ingestion (Freshness check)
    cur.execute("""
        SELECT symbol, MAX(ts) 
        FROM market_ticks 
        WHERE ts > NOW() - INTERVAL '1 hour'
        GROUP BY symbol
    """)
    ticks = cur.fetchall()
    summary += "\n--- DATA FRESHNESS (Last 1h) ---\n"
    if not ticks:
        summary += "  ⚠️ NO RECENT TICKS FOUND IN LAST HOUR!\n"
    for sym, last_ts in ticks:
        # Calculate delay
        delay = (datetime.now(timezone.utc) - last_ts).total_seconds()
        status = "✅ OK" if delay < 60 else "⚠️ STALE"
        summary += f"  {sym:<10}: {status} (Last: {last_ts.strftime('%H:%M:%S')} UTC)\n"

    # 3. Execution Latency (Performance check)
    try:
        cur.execute("""
            SELECT count(*) 
            FROM information_schema.views 
            WHERE table_name = 'v_execution_latency'
        """)
        if cur.fetchone()[0] > 0:
            cur.execute("SELECT AVG(latency_ms) FROM v_execution_latency WHERE filled_at > NOW() - INTERVAL '24 hours'")
            avg_lat = cur.fetchone()[0]
            summary += f"\n--- EXECUTION PERFORMANCE ---\n  Avg Latency (24h): {f'{avg_lat:.2f} ms' if avg_lat else 'N/A'}\n"
        else:
            summary += "\n--- EXECUTION PERFORMANCE ---\n  v_execution_latency view (DAG 19) not found.\n"
    except Exception as e:
        summary += f"\n--- EXECUTION PERFORMANCE ---\n  Error checking latency: {e}\n"

    cur.close()
    conn.close()
    print(summary)
    return summary

# =============================================================================
# SUMMARY CONSOLIDATOR
# =============================================================================

def master_summary(**context):
    ti = context["ti"]
    
    print("=" * 65)
    print(f"  FACTORY MASTER AUDIT -- {datetime.now(timezone.utc)}")
    print("=" * 65)
    
    def get_st(tid): 
        val = str(ti.xcom_pull(task_ids=tid) or "")
        if not val or "FAIL" in val or "NOT_FOUND" in val or "NO_BACKUP" in val or "unhealthy" in val or "Exited" in val: return "❌ FAIL"
        return "✅ OK"
    
    def get_val(tid): return ti.xcom_pull(task_ids=tid) or "0"

    # Consolidate results
    net = get_st("check_internet")
    binance = get_st("check_binance_api")
    kraken = get_st("check_kraken_api")
    deriv = get_st("check_deriv_api")
    ibkr = get_st("check_ibkr_bridge")
    db = get_st("audit_db_version")
    backup = get_st("audit_backups")
    
    # Node Health (Simplified)
    h_brain = get_st("audit_docker_brain")
    h_data = get_st("audit_docker_data")
    h_comp = get_st("audit_docker_compute")
    h_lab = get_st("audit_docker_lab")
    
    # Log Errors
    err_brain = get_val("audit_logs_brain")
    err_data = get_val("audit_logs_data")
    err_comp = get_val("audit_logs_compute")
    
    strat_health = ti.xcom_pull(task_ids="audit_strategy_health") or "No strategy health data available."
    
    print(f"\n[NETWORK ] Internet: {net} | Binance: {binance} | Kraken: {kraken} | Deriv: {deriv} | IBKR: {ibkr}")
    print(f"[SECURITY] DB Connection: {db} | Backup Today: {backup}")
    print(f"[NODES   ] Brain: {h_brain} | Data: {h_data} | Compute: {h_comp} | Lab: {h_lab}")
    print(f"[LOGS    ] Errors (15m) -> Brain: {err_brain} | Data: {err_data} | Compute: {err_comp}")
    print("\nCheck 'audit_cluster_inventory' task for full container list.")
    print(strat_health)
    print("=" * 65)

def audit_cluster_inventory(**context):
    ti = context["ti"]
    nodes = {
        "BRAIN (.200)": "audit_docker_brain",
        "DATA (.201)": "audit_docker_data",
        "COMPUTE (.202)": "audit_docker_compute",
        "LAB (.203)": "audit_docker_lab"
    }
    
    inventory = "\n" + "="*65 + "\n"
    inventory += "      AXIO-QUANT CLUSTER INVENTORY (Docker PS)      \n"
    inventory += "="*65 + "\n"
    
    for name, tid in nodes.items():
        output = ti.xcom_pull(task_ids=tid)
        inventory += f"\n>>> {name}\n"
        inventory += f"{'CONTAINER':<35} | {'STATUS':<25}\n"
        inventory += "-"*65 + "\n"
        if output:
            # Format output for better alignment
            for line in str(output).split('\n'):
                if '\t' in line:
                    cname, cst = line.split('\t', 1)
                    inventory += f"{cname:<35} | {cst:<25}\n"
                elif line.strip():
                    inventory += f"{line}\n"
        else:
            inventory += "  No containers active or connection failed.\n"
    
    print(inventory)
    return inventory

# =============================================================================
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id="00_factory_master_audit",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
    tags=["infra", "monitor", "v3", "master"],
) as dag:

    # 1. Network & Connectivity
    task_internet = PythonOperator(task_id="check_internet", python_callable=check_internet)
    task_nats     = PythonOperator(task_id="check_nats_client", python_callable=check_nats_client)
    task_binance  = PythonOperator(task_id="check_binance_api", python_callable=check_binance_api)
    task_kraken   = PythonOperator(task_id="check_kraken_api", python_callable=check_kraken_api)
    task_deriv    = PythonOperator(task_id="check_deriv_api", python_callable=check_deriv_api)
    task_ibkr     = PythonOperator(task_id="check_ibkr_bridge", python_callable=check_ibkr_bridge)

    # 2. Infrastructure Health (SSH)
    audit_ssh_brain = SSHOperator(
        task_id="audit_ssh_brain",
        ssh_conn_id="PROXMOX_BRAIN",
        command="uptime && free -h | grep Mem",
    )

    audit_docker_brain = SSHOperator(
        task_id="audit_docker_brain",
        ssh_conn_id="PROXMOX_BRAIN",
        command="sudo docker ps --format '{{ '{{' }}.Names{{ '}}' }}\t{{ '{{' }}.Status{{ '}}' }}'",
    )

    audit_docker_data = SSHOperator(
        task_id="audit_docker_data",
        ssh_conn_id="PROXMOX_DATA",
        command="sudo docker ps --format '{{ '{{' }}.Names{{ '}}' }}\t{{ '{{' }}.Status{{ '}}' }}'",
    )

    audit_docker_compute = SSHOperator(
        task_id="audit_docker_compute",
        ssh_conn_id="PROXMOX_COMPUTE",
        command="sudo docker ps --format '{{ '{{' }}.Names{{ '}}' }}\t{{ '{{' }}.Status{{ '}}' }}'",
    )

    audit_docker_lab = SSHOperator(
        task_id="audit_docker_lab",
        ssh_conn_id="PROXMOX_LAB",
        command="sudo docker ps --format '{{ '{{' }}.Names{{ '}}' }}\t{{ '{{' }}.Status{{ '}}' }}'",
    )

    # 2c. Log Scanner (Error detection - Last 15m)
    log_cmd = 'sudo docker ps -q | xargs -I {} sudo docker logs --since 15m 2>&1 {} | grep -ciE "error|exception|critical" || echo 0'
    
    audit_logs_brain = SSHOperator(task_id="audit_logs_brain", ssh_conn_id="PROXMOX_BRAIN", command=log_cmd)
    audit_logs_data  = SSHOperator(task_id="audit_logs_data",  ssh_conn_id="PROXMOX_DATA",  command=log_cmd)
    audit_logs_comp  = SSHOperator(task_id="audit_logs_compute", ssh_conn_id="PROXMOX_COMPUTE", command=log_cmd)

    # 2d. Backup Integrity
    audit_backups = SSHOperator(
        task_id="audit_backups",
        ssh_conn_id="PROXMOX_DATA",
        command='sudo mkdir -p /opt/platform/backups && sudo find /opt/platform/backups -name "*.sql*" -mtime -1 | grep . || echo "NO_BACKUP_TODAY"',
    )

    # 3. Data & Strategy Health
    audit_db_version = PostgresOperator(
        task_id="audit_db_version",
        postgres_conn_id=DB_CONN,
        sql="SELECT version();",
    )

    audit_strategy_health_task = PythonOperator(
        task_id="audit_strategy_health",
        python_callable=audit_strategy_health,
    )

    audit_cluster_inventory_task = PythonOperator(
        task_id="audit_cluster_inventory",
        python_callable=audit_cluster_inventory,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # 4. Master Summary
    final_summary = PythonOperator(
        task_id="master_summary",
        python_callable=master_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Flow
    [task_internet, audit_ssh_brain] >> audit_docker_brain
    audit_docker_brain >> [audit_docker_data, audit_docker_compute, audit_docker_lab, task_nats, task_binance, task_kraken, task_deriv, task_ibkr]
    [audit_docker_brain, audit_docker_data, audit_docker_compute, audit_docker_lab] >> audit_cluster_inventory_task
    [audit_docker_data, audit_docker_compute, audit_docker_lab] >> audit_logs_brain
    audit_docker_data >> [audit_logs_data, audit_logs_comp, audit_backups]
    [audit_cluster_inventory_task, audit_backups, audit_db_version] >> audit_strategy_health_task >> final_summary
