"""
=============================================================================
DAG: 98_configure_strategy
=============================================================================
Configura dinámicamente el modo de contrato, cuenta de broker y parámetros
de riesgo (SL/TP, multiplier) para cada estrategia/símbolo, sin reiniciar
ningún contenedor.

Los cambios se escriben en Redis y el bridge_provider los lee en cada orden.
Los tokens de API NUNCA pasan por este DAG — solo se usan aliases seguros.

PARÁMETROS (Airflow UI → Trigger DAG w/ config):
  symbol         : "R_25" | "Step Index" | "all"
  account_alias  : "cuenta_a" | "cuenta_b" | "default"
  contract_mode  : "binary" | "multiplier"
  multiplier     : 5 | 10 | 20 | 50 | 100
  stop_loss_usd  : 1.50
  take_profit_usd: 3.00
  ttl_hours      : 24  (horas que dura la config en Redis antes de expirar)

EJEMPLO (JSON en "Trigger DAG w/ config"):
  {
    "symbol":          "Step Index",
    "account_alias":   "cuenta_b",
    "contract_mode":   "multiplier",
    "multiplier":      10,
    "stop_loss_usd":   2.0,
    "take_profit_usd": 4.0
  }
=============================================================================
"""

from __future__ import annotations
import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

log = logging.getLogger(__name__)

REDIS_URL = os.getenv("REDIS_URL", "redis://192.168.100.200:6379/0")

# Símbolos gestionados por la plataforma
KNOWN_SYMBOLS = ["R_10", "R_25", "R_75", "Step Index", "Volatility 75 Index"]

# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "axio-quant",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(seconds=10),
}

dag = DAG(
    dag_id="98_configure_strategy",
    description="Configura modo de contrato, cuenta y riesgo por símbolo — sin reiniciar contenedores",
    schedule_interval=None,           # Solo manual
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["platform", "strategy", "config", "deriv"],
    default_args=DEFAULT_ARGS,
    params={
        "symbol": Param(
            "Step Index",
            type="string",
            description="Símbolo a configurar. Usa 'all' para todos los símbolos."
        ),
        "account_alias": Param(
            "default",
            type="string",
            description="Alias de la cuenta Deriv (debe existir en env vars del bridge)."
        ),
        "contract_mode": Param(
            "binary",
            type="string",
            description="Modo: 'binary' (CALL/PUT 5m) o 'multiplier' (MULTUP/MULTDOWN con SL/TP)."
        ),
        "multiplier": Param(
            10,
            type="integer",
            description="Apalancamiento (solo modo multiplier). Ej: 5, 10, 20, 50, 100."
        ),
        "stop_loss_usd": Param(
            1.50,
            type="number",
            description="Stop Loss en USD por contrato (solo modo multiplier)."
        ),
        "take_profit_usd": Param(
            3.00,
            type="number",
            description="Take Profit en USD por contrato (solo modo multiplier)."
        ),
        "ttl_hours": Param(
            24,
            type="integer",
            description="Horas que dura esta config en Redis. 0 = permanente."
        ),
    },
)

# ──────────────────────────────────────────────────────────────────────────────

def _get_redis_client():
    import redis  # lazy import para no bloquear carga del DAG
    client = redis.from_url(REDIS_URL)
    client.ping()
    return client


def apply_config(**context) -> None:
    params         = context["params"]
    symbol         = params["symbol"]
    account_alias  = params["account_alias"].strip().lower()
    contract_mode  = params["contract_mode"]
    multiplier     = int(params["multiplier"])
    stop_loss_usd  = float(params["stop_loss_usd"])
    take_profit_usd = float(params["take_profit_usd"])
    ttl_hours      = int(params["ttl_hours"])
    ttl_sec        = ttl_hours * 3600 if ttl_hours > 0 else None

    r = _get_redis_client()

    symbols = KNOWN_SYMBOLS if symbol == "all" else [symbol]

    applied = []
    for sym in symbols:
        keys = {
            f"bridge:active_account:{sym}": account_alias,
            f"bridge:contract_mode:{sym}":  contract_mode,
            f"bridge:multiplier:{sym}":     str(multiplier),
            f"bridge:sl_usd:{sym}":         str(stop_loss_usd),
            f"bridge:tp_usd:{sym}":         str(take_profit_usd),
        }
        for key, val in keys.items():
            if ttl_sec:
                r.set(key, val, ex=ttl_sec)
            else:
                r.set(key, val)

        applied.append({
            "symbol":          sym,
            "account_alias":   account_alias,
            "contract_mode":   contract_mode,
            "multiplier":      multiplier,
            "stop_loss_usd":   stop_loss_usd,
            "take_profit_usd": take_profit_usd,
            "ttl_hours":       ttl_hours if ttl_hours > 0 else "∞ permanente",
        })
        log.info(f"✅ {sym}: account={account_alias} mode={contract_mode} ×{multiplier} SL=${stop_loss_usd} TP=${take_profit_usd}")

    log.info(f"\n📋 RESUMEN DE CONFIGURACIÓN APLICADA:\n{json.dumps(applied, indent=2, ensure_ascii=False)}")


def verify_config(**context) -> None:
    """Muestra la config actual de Redis como verificación."""
    params  = context["params"]
    symbol  = params["symbol"]
    symbols = KNOWN_SYMBOLS if symbol == "all" else [symbol]
    r       = _get_redis_client()

    log.info("🔍 CONFIGURACIÓN ACTUAL EN REDIS:")
    for sym in symbols:
        mode    = r.get(f"bridge:contract_mode:{sym}")
        account = r.get(f"bridge:active_account:{sym}")
        mul     = r.get(f"bridge:multiplier:{sym}")
        sl      = r.get(f"bridge:sl_usd:{sym}")
        tp      = r.get(f"bridge:tp_usd:{sym}")
        ttl     = r.ttl(f"bridge:contract_mode:{sym}")

        log.info(
            f"  {sym:20s} | "
            f"account={account.decode() if account else 'default':12s} | "
            f"mode={mode.decode() if mode else 'binary':10s} | "
            f"×{mul.decode() if mul else '?':4s} | "
            f"SL=${sl.decode() if sl else '?':5s} | "
            f"TP=${tp.decode() if tp else '?':5s} | "
            f"TTL={ttl}s"
        )


# ── TASKS ─────────────────────────────────────────────────────────────────────

t_apply = PythonOperator(
    task_id="apply_config_to_redis",
    python_callable=apply_config,
    dag=dag,
)

t_verify = PythonOperator(
    task_id="verify_redis_config",
    python_callable=verify_config,
    dag=dag,
)

t_apply >> t_verify
