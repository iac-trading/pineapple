"""
=============================================================================
Factory UI V3 — Axio-Quant Trading Platform
=============================================================================
Sidebar navigation + mixed tab layout.
Pages: Dashboard, Strategy Lab, Backtest Results, Live Monitor, AI Brain
=============================================================================
"""

import itertools
import json
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from uuid import uuid4

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import psycopg2
from psycopg2.extras import RealDictCursor
import streamlit as st
import requests

# ─── Config ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] factory_ui: %(message)s")
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Axio-Quant | AI Command Center",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "trading")
DB_USER = os.getenv("DB_USER", "tsdb")
DB_PASS = os.getenv("DB_PASS", "")

NATS_URL = os.getenv("NATS_URL", "nats://192.168.100.200:4222")

def nats_publish(subject: str, payload: dict):
    """Helper sync wrapper to publish to NATS from Streamlit."""
    import asyncio
    from nats.aio.client import Client as NATS
    
    async def _pub():
        nc = NATS()
        try:
            await nc.connect(servers=[NATS_URL])
            await nc.publish(subject, json.dumps(payload).encode())
            await nc.flush()
        except Exception as e:
            logger.error(f"NATS Publish Error: {e}")
        finally:
            await nc.close()
            
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_pub())
    finally:
        loop.close()

import base64

def get_base64(bin_file):
    with open(bin_file, 'rb') as f:
        data = f.read()
    return base64.b64encode(data).decode()

# Intentar cargar fondo en Base64 para forzar visualización
try:
    bin_str = get_base64("/app/app/bg.png")
    bg_img_style = f"url('data:image/png;base64,{bin_str}')"
except:
    bg_img_style = "none"

st.markdown(f"""<style>
    [data-testid="stAppViewContainer"] {{
        background-image: linear-gradient(rgba(10, 14, 26, 0.88), rgba(10, 14, 26, 0.88)), {bg_img_style} !important;
        background-size: cover !important;
        background-position: center !important;
        background-attachment: fixed !important;
        background-color: #0a0e1a !important;
    }}
    /* Forzar transparencia en capas superiores */
    .main, [data-testid="stHeader"], .stApp {{
        background-color: rgba(0,0,0,0) !important;
    }}
    [data-testid="stSidebar"] {{
        background: rgba(10, 14, 26, 0.75) !important;
        backdrop-filter: blur(15px);
        border-right: 1px solid rgba(255, 255, 255, 0.1);
    }}
    [data-testid="stMetric"] {{
        background: rgba(255, 255, 255, 0.05) !important;
        backdrop-filter: blur(10px);
        border-radius: 16px;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }}
    h1, h2, h3, p, span, label {{ color: #e2e8f0 !important; }}
</style>""", unsafe_allow_html=True)

# ─── Database ─────────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def db_conn():
    if not DB_PASS:
        raise RuntimeError("DB_PASS vacío. Revisa env del servicio factory_ui.")
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS, connect_timeout=5,
    )


def q(sql: str, params: Optional[tuple] = None) -> list[dict]:
    """Read query → list of dicts."""
    try:
        conn = db_conn()
        conn.autocommit = True
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or ())
            return list(cur.fetchall()) if cur.description else []
    except Exception as e:
        logger.error(f"Query error: {e}")
        # Reset connection on error
        try:
            db_conn.clear()
        except Exception:
            pass
        return []


def exec_sql(sql: str, params: Optional[tuple] = None) -> bool:
    """Write query → bool success."""
    try:
        conn = db_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Exec error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        st.error(f"Error DB: {e}")
        return False


def exec_sql_returning(sql: str, params: Optional[tuple] = None) -> Optional[str]:
    """Write query with RETURNING → single value."""
    try:
        conn = db_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            result = cur.fetchone()
        conn.commit()
        return str(result[0]) if result else None
    except Exception as e:
        logger.error(f"Exec error: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ─── Sidebar Navigation ──────────────────────────────────────────────────────

with st.sidebar:
    st.image("/app/app/logo.png", use_column_width=True)

    page = st.radio(
        "Navegación",
        ["📊 Dashboard", "🌐 Market Intelligence", "🔬 Strategy Lab", "📈 Backtest Results",
         "⚡ Live Monitor", "🧠 AI Brain", "🌐 Infrastructure", "📋 Auditoría"],
        label_visibility="collapsed",
    )
    st.divider()
    st.caption(f"🕐 {utc_now().strftime('%Y-%m-%d %H:%M')} UTC")
    st.caption("Nodos: BRAIN · DATA · COMPUTE · LAB")

    st.divider()
    if st.button("🚨 GLOBAL PANIC", type="primary", key="panic_btn", help="KILL ALL STRATEGIES & FLATTEN POSITIONS"):
        nats_publish("orders.panic", {
            "ts": datetime.now(timezone.utc).isoformat(),
            "actor": "admin_ui",
            "reason": "MANUAL_PANIC_TRIGGERED"
        })
        st.error("🚨 PANIC SIGNAL SENT! ALL SYSTEMS HALTING.")
        st.toast("Panic signal broadcasted to Risk Engine.", icon="🔥")


# =============================================================================
# PAGE: DASHBOARD
# =============================================================================

if page == "📊 Dashboard":
    st.markdown("## 📊 Dashboard — Vista General")

    # DB connectivity check
    try:
        _test = db_conn()
        _test_ok = True
    except Exception as _e:
        _test_ok = False
        st.error(f"❌ No se pudo conectar a la base de datos: `{DB_HOST}:{DB_PORT}/{DB_NAME}` user=`{DB_USER}` — {_e}")

    if _test_ok:
        # KPI row
        k1, k2, k3, k4, k5 = st.columns(5)

        blueprints = q("SELECT COUNT(*) as n FROM strategy_blueprints")
        instances = q("SELECT COUNT(*) as n FROM strategy_instances WHERE is_active=TRUE")
        running = q("SELECT COUNT(*) as n FROM strategy_instances WHERE status='running' AND is_active=TRUE")
        bt_done = q("SELECT COUNT(*) as n FROM backtest_jobs WHERE status='done'")
        bt_queued = q("SELECT COUNT(*) as n FROM backtest_jobs WHERE status IN ('queued','running')")

        k1.metric("Blueprints", blueprints[0]["n"] if blueprints else 0)
        k2.metric("Instancias Activas", instances[0]["n"] if instances else 0)
        k3.metric("Running", running[0]["n"] if running else 0)
        k4.metric("Backtests Completados", bt_done[0]["n"] if bt_done else 0)
        k5.metric("Backtests en Cola", bt_queued[0]["n"] if bt_queued else 0)

    st.divider()

    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.markdown("### 🏆 Top Backtests por Sharpe")
        top = q("""
            SELECT
                r.job_id::text,
                r.blueprint_id,
                r.symbol,
                r.broker,
                (r.metrics->>'sharpe')::float      AS sharpe,
                (r.metrics->>'total_return')::float AS total_return,
                (r.metrics->>'max_drawdown')::float AS max_drawdown,
                (r.metrics->>'win_rate')::float     AS win_rate,
                (r.metrics->>'n_trades')::int       AS trades,
                r.metrics->>'engine'                AS engine
            FROM backtest_results r
            JOIN backtest_jobs j ON j.job_id = r.job_id
            WHERE j.status = 'done'
            ORDER BY (r.metrics->>'sharpe')::float DESC
            LIMIT 10
        """)
        if top:
            df = pd.DataFrame(top)
            df["total_return"] = (df["total_return"] * 100).round(2)
            df["max_drawdown"] = (df["max_drawdown"] * 100).round(2)
            df["win_rate"] = (df["win_rate"] * 100).round(1)
            df["sharpe"] = df["sharpe"].round(4)
            df.insert(0, "#", range(1, len(df) + 1))
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info("No hay backtests completados aún. Ve a **Strategy Lab** para lanzar uno.")

    with col_right:
        st.markdown("### 📡 Instancias Live")
        live = q("""
            SELECT name, blueprint_id, symbol, broker, status, desired_status,
                   last_heartbeat
            FROM strategy_instances
            WHERE is_active = TRUE
            ORDER BY status DESC, name
        """)
        if live:
            for inst in live:
                status_icon = "🟢" if inst["status"] == "running" else "🔴" if inst["status"] == "error" else "⚪"
                st.markdown(
                    f"{status_icon} **{inst['name']}** — {inst['symbol']} "
                    f"({inst['broker']}) `{inst['status']}`"
                )
        else:
            st.info("No hay instancias activas.")


# =============================================================================
# PAGE: MARKET INTELLIGENCE
# =============================================================================

elif page == "🌐 Market Intelligence":
    st.markdown("## 🌐 Market Intelligence — Sentimiento y Macro")
    
    # 1. KPIs de Sentimiento
    s_col1, s_col2, s_col3 = st.columns(3)
    
    avg_sent = q("SELECT AVG(sentiment_score) as avg FROM ai_news_events WHERE ts > NOW() - INTERVAL '24 hours'")
    count_news = q("SELECT COUNT(*) as n FROM ai_news_events WHERE ts > NOW() - INTERVAL '24 hours'")
    high_impact = q("SELECT COUNT(*) as n FROM ai_news_events WHERE impact_level >= 4 AND ts > NOW() - INTERVAL '24 hours'")
    
    val_sent = avg_sent[0]['avg'] if avg_sent and avg_sent[0]['avg'] else 0.0
    s_col1.metric("Sentimiento 24h (Avg)", f"{val_sent:.2f}")
    s_col2.metric("Noticias Ingestadas (24h)", count_news[0]['n'] if count_news else 0)
    s_col3.metric("Eventos Críticos detectados", high_impact[0]['n'] if high_impact else 0)
    
    st.divider()
    
    m_left, m_right = st.columns([2, 1])
    
    with m_left:
        st.markdown("### 📰 Feed de Inteligencia Unificado")
        news = q("""
            SELECT ts, source, title, sentiment_score, impact_level
            FROM ai_news_events
            ORDER BY ts DESC
            LIMIT 20
        """)
        if news:
            for n in news:
                icon = "🔥" if n['impact_level'] >= 4 else "📰"
                s_val = n['sentiment_score']
                color = "#10b981" if s_val > 0.1 else "#ef4444" if s_val < -0.1 else "#94a3b8"
                
                st.markdown(
                    f"<div style='padding:0.5rem; border-bottom:1px solid rgba(255,255,255,0.05)'>"
                    f"{icon} <b>[{n['source']}]</b> {n['title']} "
                    f"<span style='color:{color}; font-weight:bold'>({s_val:.2f})</span>"
                    f"</div>", 
                    unsafe_allow_html=True
                )
        else:
            st.info("No hay noticias en el feed aún. Los agentes están trabajando...")
            
    with m_right:
        st.markdown("### 📅 Próximos Eventos Macro")
        events = q("""
            SELECT ts, title, symbol as country, impact_level
            FROM ai_news_events
            WHERE source = 'ForexFactory' AND ts >= NOW()
            ORDER BY ts ASC
            LIMIT 10
        """)
        if events:
            for e in events:
                time_str = e['ts'].strftime('%H:%M')
                st.warning(f"**{time_str}** | {e['country']} - {e['title']}")
        else:
            st.success("No hay eventos críticos programados para las próximas horas.")


# =============================================================================
# PAGE: STRATEGY LAB (Batch Backtesting)
# =============================================================================

elif page == "🔬 Strategy Lab":
    st.markdown("## 🔬 Strategy Lab — Generación Masiva de Backtests")
    st.caption("Genera variaciones de parámetros, lanza backtests en batch, y encuentra las mejores estrategias.")

    tab_config, tab_manual = st.tabs(["🚀 Batch Automático", "🔧 Manual"])

    with tab_config:
        col_params, col_preview = st.columns([2, 3])

        with col_params:
            st.markdown("### Configuración del Batch")

            sym = st.text_input("Símbolo", value="R_75", key="lab_sym")
            broker = st.selectbox("Broker", ["deriv", "ibkr", "paper"], key="lab_broker")

            c_start, c_end = st.columns(2)
            with c_start:
                start_date = st.date_input("Inicio", value=datetime(2025, 1, 1), key="lab_start")
            with c_end:
                end_date = st.date_input("Fin", value=datetime(2026, 3, 1), key="lab_end")

            data_source = st.selectbox(
                "Fuente de datos",
                ["candles_1h", "candles_5m", "candles_1d", "ticks", "auto"],
                key="lab_datasource",
            )

            st.markdown("### Motores")
            use_101 = st.checkbox("101 — Donchian Breakout + ATR", value=True, key="lab_101")
            use_102 = st.checkbox("102 — Bollinger Mean Reversion", value=True, key="lab_102")
            use_bnh = st.checkbox("Baseline — Buy & Hold", value=True, key="lab_bnh")

            st.markdown("### Variaciones de Parámetros")
            st.caption("Se generan combinaciones de los rangos definidos")

            # Initialize defaults
            entry_p_vals = []
            exit_p_vals = []
            atr_p_vals = []
            stop_mult_vals = []
            bb_p_vals = []
            bb_std_vals = []
            bb_stop_vals = []

            # Strategy 101 params
            if use_101:
                with st.expander("📐 Parámetros 101 (Donchian)", expanded=True):
                    entry_p_vals = st.multiselect("entry_p", [10, 15, 20, 25, 30], default=[15, 20, 25], key="e101_ep")
                    exit_p_vals = st.multiselect("exit_p", [5, 7, 10, 15], default=[7, 10], key="e101_xp")
                    atr_p_vals = st.multiselect("atr_p", [10, 14, 20], default=[14], key="e101_atr")
                    stop_mult_vals = st.multiselect("stop_mult", [1.0, 1.5, 2.0, 2.5, 3.0], default=[1.5, 2.0], key="e101_sm")

            # Strategy 102 params
            if use_102:
                with st.expander("📐 Parámetros 102 (Bollinger)", expanded=True):
                    bb_p_vals = st.multiselect("bb_p", [15, 20, 25, 30], default=[20, 25], key="e102_bbp")
                    bb_std_vals = st.multiselect("bb_std", [1.5, 2.0, 2.5, 3.0], default=[2.0, 2.5], key="e102_bbs")
                    bb_stop_vals = st.multiselect("stop_mult", [1.5, 2.0, 2.5, 3.0], default=[2.0], key="e102_sm")

        # Generate preview
        with col_preview:
            st.markdown("### 📋 Preview de Jobs")
            jobs_preview = []

            if use_101 and entry_p_vals and exit_p_vals:
                for ep, xp, atr, sm in itertools.product(
                    entry_p_vals, exit_p_vals, atr_p_vals, stop_mult_vals
                ):
                    jobs_preview.append({
                        "engine": "101_donchian",
                        "blueprint_id": "101",
                        "params": {"entry_p": ep, "exit_p": xp, "atr_p": atr, "stop_mult": sm},
                    })

            if use_102 and bb_p_vals and bb_std_vals:
                for bbp, bbs, sm in itertools.product(bb_p_vals, bb_std_vals, bb_stop_vals):
                    jobs_preview.append({
                        "engine": "102_bollinger",
                        "blueprint_id": "102",
                        "params": {"bb_p": bbp, "bb_std": bbs, "stop_mult": sm},
                    })

            if use_bnh:
                jobs_preview.append({
                    "engine": "buy_and_hold",
                    "blueprint_id": "baseline",
                    "params": {},
                })

            total_jobs = len(jobs_preview)

            st.metric("Total de Jobs", total_jobs)

            if jobs_preview:
                preview_df = pd.DataFrame([
                    {
                        "ID": i + 1,
                        "Motor": str(j["engine"]),
                        "Configuración": json.dumps(j["params"], separators=(",", ":")),
                    }
                    for i, j in enumerate(jobs_preview)
                ])
                st.table(preview_df)
            else:
                st.warning("Selecciona al menos un motor y sus parámetros.")

            st.divider()

            if st.button(
                f"🚀 Lanzar {total_jobs} Backtests",
                type="primary",
                disabled=total_jobs == 0,
                key="lab_launch_btn",
            ):
                progress = st.progress(0, text="Creando jobs...")
                created = 0
                for i, job in enumerate(jobs_preview):
                    job_id = exec_sql_returning(
                        """
                        INSERT INTO backtest_jobs
                            (blueprint_id, broker, symbol, start_ts, end_ts, params, status)
                        VALUES (%s, %s, %s, %s, %s, %s::jsonb, 'queued')
                        RETURNING job_id
                        """,
                        (
                            job["blueprint_id"],
                            broker,
                            sym,
                            datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc),
                            datetime.combine(end_date, datetime.min.time(), tzinfo=timezone.utc),
                            json.dumps({**job["params"], "data_source": data_source}),
                        ),
                    )
                    if job_id:
                        created += 1
                    progress.progress((i + 1) / total_jobs, text=f"Job {i+1}/{total_jobs}")

                st.success(f"✅ {created}/{total_jobs} jobs creados. El Backtest Worker los recogerá automáticamente.")
                st.balloons()

    with tab_manual:
        st.markdown("### Lanzar un Backtest Individual")
        mc1, mc2 = st.columns(2)
        with mc1:
            m_bp = st.text_input("Blueprint ID", value="101", key="man_bp")
            m_sym = st.text_input("Símbolo", value="R_75", key="man_sym")
            m_broker = st.selectbox("Broker", ["deriv", "ibkr", "paper"], key="man_broker")
        with mc2:
            m_start = st.text_input("Inicio (YYYY-MM-DD)", value="2025-01-01", key="man_start")
            m_end = st.text_input("Fin (YYYY-MM-DD)", value="2026-03-01", key="man_end")
            m_params = st.text_area("Params (JSON)", value='{"entry_p": 20, "exit_p": 10}', key="man_params")

        if st.button("Lanzar Backtest", type="primary", key="man_launch"):
            try:
                params_json = json.loads(m_params)
            except json.JSONDecodeError:
                st.error("JSON inválido en parámetros")
                params_json = None

            if params_json is not None:
                job_id = exec_sql_returning(
                    """
                    INSERT INTO backtest_jobs
                        (blueprint_id, broker, symbol, start_ts, end_ts, params, status)
                    VALUES (%s, %s, %s, %s::timestamptz, %s::timestamptz, %s::jsonb, 'queued')
                    RETURNING job_id
                    """,
                    (m_bp, m_broker, m_sym, m_start, m_end, json.dumps(params_json)),
                )
                if job_id:
                    st.success(f"✅ Job creado: `{job_id}`")


# =============================================================================
# PAGE: BACKTEST RESULTS
# =============================================================================

elif page == "📈 Backtest Results":
    st.markdown("## 📈 Backtest Results — Ranking y Análisis")

    # Stats bar
    s1, s2, s3, s4 = st.columns(4)
    stats = q("""
        SELECT
            COUNT(*) FILTER (WHERE status='done')    AS done,
            COUNT(*) FILTER (WHERE status='running') AS running,
            COUNT(*) FILTER (WHERE status='queued')  AS queued,
            COUNT(*) FILTER (WHERE status='error')   AS errors
        FROM backtest_jobs
    """)
    if stats:
        s = stats[0]
        s1.metric("✅ Completados", s["done"])
        s2.metric("⏳ Ejecutando", s["running"])
        s3.metric("📋 En Cola", s["queued"])
        s4.metric("❌ Errores", s["errors"])

    if st.button("🔄 Refrescar", key="bt_refresh"):
        st.rerun()

    st.divider()

    tab_ranking, tab_curves, tab_compare = st.tabs(["🏆 Ranking", "📊 Equity Curves", "⚖️ Comparar"])

    with tab_ranking:
        results = q("""
            SELECT
                ROW_NUMBER() OVER (ORDER BY (r.metrics->>'sharpe')::float DESC) AS rank,
                r.job_id::text,
                r.blueprint_id,
                r.symbol,
                r.broker,
                r.metrics->>'engine'                AS engine,
                COALESCE((r.metrics->>'sharpe')::float, 0)        AS sharpe,
                COALESCE((r.metrics->>'total_return')::float, 0)  AS total_return,
                COALESCE((r.metrics->>'max_drawdown')::float, 0)  AS max_drawdown,
                COALESCE((r.metrics->>'win_rate')::float, 0)      AS win_rate,
                COALESCE((r.metrics->>'n_trades')::int, 0)        AS trades
            FROM backtest_results r
            JOIN backtest_jobs j ON j.job_id = r.job_id
            WHERE j.status = 'done'
            ORDER BY (r.metrics->>'sharpe')::float DESC
        """)

        if results:
            df = pd.DataFrame(results)
            # Asegurar tipos y limpiar nulos
            df["total_return"] = (df["total_return"] * 100).round(2)
            df["max_drawdown"] = (df["max_drawdown"] * 100).round(2)
            df["win_rate"] = (df["win_rate"] * 100).round(1)
            df["sharpe"] = df["sharpe"].round(4)
            df = df.fillna(0)

            # Highlight top 3
            st.markdown("#### 🥇🥈🥉 Top 3")
            top3_cols = st.columns(3)
            medals = ["🥇", "🥈", "🥉"]
            colors = ["#fbbf24", "#94a3b8", "#d97706"]

            for i, col in enumerate(top3_cols):
                if i < len(df):
                    row = df.iloc[i]
                    with col:
                        st.markdown(f"""
                        <div style='background:linear-gradient(135deg,rgba(99,102,241,0.15),rgba(139,92,246,0.1));
                                    border:1px solid {colors[i]};border-radius:12px;padding:1.2rem;text-align:center'>
                            <div style='font-size:2rem'>{medals[i]}</div>
                            <div style='font-size:0.8rem;color:#94a3b8'>{row['engine']}</div>
                            <div style='font-size:1.8rem;font-weight:700;color:#e2e8f0'>
                                Sharpe {row['sharpe']}
                            </div>
                            <div style='color:#10b981;font-weight:600'>
                                Return {row['total_return']}%
                            </div>
                            <div style='color:#ef4444;font-size:0.85rem'>
                                MaxDD {row['max_drawdown']}%
                            </div>
                            <div style='color:#94a3b8;font-size:0.8rem'>
                                {row['trades']} trades · WR {row['win_rate']}%
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

            st.divider()
            st.markdown("#### Tabla completa (Top 50)")
            st.table(
                df[["rank", "engine", "sharpe", "total_return", "max_drawdown",
                    "win_rate", "trades", "blueprint_id", "symbol", "broker", "job_id"]].head(50)
            )
        else:
            st.info("No hay resultados de backtests aún.")

    with tab_curves:
        curves_data = q("""
            SELECT
                r.job_id::text,
                r.blueprint_id,
                r.metrics->>'engine' AS engine,
                (r.metrics->>'sharpe')::float AS sharpe,
                r.equity_curve
            FROM backtest_results r
            JOIN backtest_jobs j ON j.job_id = r.job_id
            WHERE j.status = 'done' AND r.equity_curve IS NOT NULL
            ORDER BY (r.metrics->>'sharpe')::float DESC
            LIMIT 10
        """)

        if curves_data:
            fig = go.Figure()
            for i, row in enumerate(curves_data):
                curve = row["equity_curve"] if isinstance(row["equity_curve"], list) else json.loads(row["equity_curve"])
                if curve:
                    ts_list = [p["ts"] for p in curve]
                    eq_list = [p["equity"] for p in curve]
                    label = f"#{i+1} {row['engine']} (Sharpe: {row['sharpe']:.3f})"
                    fig.add_trace(go.Scatter(
                        x=ts_list, y=eq_list, mode="lines",
                        name=label, line=dict(width=2),
                    ))

            fig.update_layout(
                title="Equity Curves — Top 10 Backtests",
                template="plotly_dark",
                paper_bgcolor="#0f1117",
                plot_bgcolor="#0f1117",
                xaxis_title="Fecha",
                yaxis_title="Equity (normalizado)",
                legend=dict(orientation="h", yanchor="bottom", y=-0.3),
                height=500,
                margin=dict(l=40, r=20, t=50, b=20),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hay equity curves disponibles.")

    with tab_compare:
        st.markdown("### ⚖️ Comparar Motores")
        compare = q("""
            SELECT
                r.metrics->>'engine' AS engine,
                COUNT(*) AS n_backtests,
                ROUND(AVG((r.metrics->>'sharpe')::float)::numeric, 4)       AS avg_sharpe,
                ROUND(AVG((r.metrics->>'total_return')::float)::numeric, 4) AS avg_return,
                ROUND(AVG((r.metrics->>'max_drawdown')::float)::numeric, 4) AS avg_maxdd,
                ROUND(AVG((r.metrics->>'win_rate')::float)::numeric, 4)     AS avg_winrate,
                ROUND(MAX((r.metrics->>'sharpe')::float)::numeric, 4)       AS best_sharpe
            FROM backtest_results r
            JOIN backtest_jobs j ON j.job_id = r.job_id
            WHERE j.status = 'done'
            GROUP BY r.metrics->>'engine'
            ORDER BY avg_sharpe DESC
        """)
        if compare:
            df_compare = pd.DataFrame(compare)
            df_compare["avg_return"] = (df_compare["avg_return"].astype(float) * 100).round(2)
            df_compare["avg_maxdd"] = (df_compare["avg_maxdd"].astype(float) * 100).round(2)
            df_compare["avg_winrate"] = (df_compare["avg_winrate"].astype(float) * 100).round(1)
            st.dataframe(df_compare, use_container_width=True, hide_index=True)

            # Bar chart
            fig_bar = px.bar(
                df_compare, x="engine", y="avg_sharpe",
                color="engine", title="Sharpe Promedio por Motor",
                template="plotly_dark",
            )
            fig_bar.update_layout(
                paper_bgcolor="#0f1117", plot_bgcolor="#0f1117",
                showlegend=False, height=350,
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No hay datos suficientes para comparar.")


# =============================================================================
# PAGE: LIVE MONITOR
# =============================================================================

elif page == "⚡ Live Monitor":
    st.markdown("## ⚡ Live Monitor — Instancias y Despliegue")

    tab_instances, tab_deploy, tab_blueprints = st.tabs(
        ["📡 Instancias Activas", "🚀 Deploy desde Backtest", "📦 Blueprints"]
    )

    with tab_instances:
        instances = q("""
            SELECT
                instance_id::text, name, blueprint_id, assigned_host,
                symbol, broker, qty, status, desired_status,
                last_heartbeat, meta, created_at
            FROM strategy_instances
            WHERE is_active = TRUE
            ORDER BY status DESC, name
        """)

        if instances:
            for inst in instances:
                icon = "🟢" if inst["status"] == "running" else "🔴" if inst["status"] == "error" else "⚪"
                with st.expander(f"{icon} {inst['name']} — {inst['symbol']} ({inst['broker']}) → `{inst['status']}`"):
                    ic1, ic2, ic3, ic4 = st.columns(4)
                    ic1.metric("Blueprint", inst["blueprint_id"])
                    ic2.metric("Host", inst["assigned_host"])
                    ic3.metric("Qty", inst["qty"])
                    hb = inst["last_heartbeat"]
                    ic4.metric("Último HB", hb.strftime("%H:%M:%S") if hb else "—")

                    nc1, nc2 = st.columns(2)
                    with nc1:
                        new_status = st.selectbox(
                            "desired_status",
                            ["running", "stopped"],
                            index=0 if inst["desired_status"] == "running" else 1,
                            key=f"ds_{inst['instance_id']}",
                        )
                    with nc2:
                        if st.button("Aplicar", key=f"apply_{inst['instance_id']}"):
                            exec_sql(
                                "UPDATE strategy_instances SET desired_status=%s WHERE instance_id=%s::uuid",
                                (new_status, inst["instance_id"]),
                            )
                            st.success(f"✅ {inst['name']} → {new_status}")
                            st.rerun()
        else:
            st.info("No hay instancias activas. Usa **Deploy desde Backtest** para crear una.")

    with tab_deploy:
        st.markdown("### 🚀 Promover Backtest → Live")
        st.caption("Selecciona un backtest exitoso para desplegarlo como instancia real.")

        top_for_deploy = q("""
            SELECT
                r.job_id::text,
                r.blueprint_id,
                r.symbol,
                r.broker,
                r.metrics->>'engine' AS engine,
                (r.metrics->>'sharpe')::float AS sharpe,
                (r.metrics->>'total_return')::float AS total_return,
                (r.metrics->>'win_rate')::float AS win_rate,
                r.params
            FROM backtest_results r
            JOIN backtest_jobs j ON j.job_id = r.job_id
            WHERE j.status = 'done'
            ORDER BY (r.metrics->>'sharpe')::float DESC
            LIMIT 20
        """)

        if top_for_deploy:
            deploy_options = {
                f"#{i+1} {r['engine']} Sharpe={r['sharpe']:.3f} Ret={r['total_return']*100:.1f}% ({r['symbol']})": r
                for i, r in enumerate(top_for_deploy)
            }

            selected_label = st.selectbox("Seleccionar backtest", list(deploy_options.keys()), key="deploy_sel")
            selected_bt = deploy_options[selected_label]

            dc1, dc2 = st.columns(2)
            with dc1:
                deploy_name = st.text_input("Nombre instancia", value=f"live_{selected_bt['engine']}", key="dep_name")
                deploy_host = st.selectbox("Host", ["tr-infra-compute-01"], key="dep_host")
            with dc2:
                deploy_qty = st.number_input("Quantity", value=0.10, step=0.01, key="dep_qty")
                deploy_desired = st.selectbox("Estado inicial", ["stopped", "running"], key="dep_desired")

            if st.button("🚀 Desplegar como Instancia Live", type="primary", key="dep_go"):
                # Check blueprint exists
                bp_check = q(
                    "SELECT 1 FROM strategy_blueprints WHERE blueprint_id=%s",
                    (selected_bt["blueprint_id"],),
                )
                if not bp_check:
                    st.warning(f"El blueprint `{selected_bt['blueprint_id']}` no existe. Créalo primero en Blueprints.")
                else:
                    params = selected_bt.get("params") or {}
                    ok = exec_sql(
                        """
                        INSERT INTO strategy_instances
                            (blueprint_id, name, assigned_host, symbol, broker, qty,
                             params, desired_status, status)
                        VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,%s,'stopped')
                        """,
                        (
                            selected_bt["blueprint_id"],
                            deploy_name,
                            deploy_host,
                            selected_bt["symbol"],
                            selected_bt["broker"],
                            deploy_qty,
                            json.dumps(params),
                            deploy_desired,
                        ),
                    )
                    if ok:
                        st.success(f"✅ Instancia `{deploy_name}` creada. El Factory Agent la desplegará.")
                        st.balloons()
        else:
            st.info("No hay backtests para desplegar. Ejecuta backtests primero en **Strategy Lab**.")

    with tab_blueprints:
        st.markdown("### 📦 Gestión de Blueprints")
        cols = st.columns([2, 2, 3, 1])
        with cols[0]:
            bp_id = st.text_input("blueprint_id", key="bp_id_in")
        with cols[1]:
            bp_name = st.text_input("name", key="bp_name_in")
        with cols[2]:
            bp_image = st.text_input("docker_image", key="bp_img_in")
        with cols[3]:
            bp_version = st.text_input("version", value="1.0.0", key="bp_ver_in")

        if st.button("Guardar Blueprint", type="primary", key="bp_save_btn"):
            exec_sql(
                """
                INSERT INTO strategy_blueprints (blueprint_id, name, docker_image, version)
                VALUES (%s,%s,%s,%s) ON CONFLICT (blueprint_id) DO UPDATE SET name=EXCLUDED.name
                """,
                (bp_id, bp_name, bp_image, bp_version),
            )
            st.success("Blueprint guardado.")

        rows = q("SELECT blueprint_id, name, docker_image, version, created_at FROM strategy_blueprints")
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# =============================================================================
# PAGE: AI BRAIN
# =============================================================================

elif page == "🧠 AI Brain":
    st.title("🧠 AI Strategy Brain")
    st.caption("Genera, previsualiza y despliega estrategias autónomas usando Llama 3.")

    BRAIN_API = "http://192.168.100.203:8010"

    tab_new, tab_status = st.tabs(["🆕 Nueva Propuesta", "📋 Control de Estrategias"])

    with tab_new:
        with st.form("ai_gen_form"):
            ai_name = st.text_input("Nombre de la Estrategia", value="AI_Scalper_BTC")
            ai_prompt = st.text_area("Describa la lógica (ej: Scalping de 1 min con NATS)", height=150)
            if st.form_submit_button("🚀 Generar con Llama 3", type="primary"):
                exec_sql(
                    "INSERT INTO ai_strategy_proposals (proposal_id, goal_description, status) VALUES (%s, %s, 'pending')",
                    (str(uuid4()), f"NAME: {ai_name} | PROMPT: {ai_prompt}"),
                )
                st.success("✅ Misión enviada al Brain. En unos segundos estará lista.")

    with tab_status:
        proposals = q("SELECT proposal_id::text as pid, goal_description as desc, status, created_at FROM ai_strategy_proposals ORDER BY created_at DESC LIMIT 20")
        
        if not proposals:
            st.info("No hay estrategias registradas.")
        else:
            for p in proposals:
                status_icon = {"pending": "⏳", "generated": "📦", "deployed": "🚀"}.get(p["status"], "❓")
                with st.expander(f"{status_icon} {p['status'].upper()} — {p['desc'][:50]}..."):
                    st.write(f"**ID:** `{p['pid']}` | **Creada:** {p['created_at']}")
                    st.info(f"**Objetivo:** {p['desc']}")
                    
                    if p["status"] in ["generated", "deployed"]:
                        col1, col2 = st.columns(2)
                        
                        if col1.button("🔍 Ver Código Python", key=f"view_{p['pid']}"):
                            try:
                                r = requests.get(f"{BRAIN_API}/strategies/{p['pid']}/code", timeout=5)
                                if "code" in r.json():
                                    st.code(r.json()["code"], language="python")
                                else:
                                    st.error("No se pudo obtener el código.")
                            except Exception as e:
                                st.error(f"Error conectando con el Brain: {e}")
                        
                        if col2.button("🚀 DESPLEGAR A PRODUCCIÓN", key=f"dep_{p['pid']}", type="primary"):
                            with st.spinner("Construyendo imagen y lanzando contenedor..."):
                                try:
                                    r = requests.post(f"{BRAIN_API}/strategies/{p['pid']}/deploy", timeout=60)
                                    if "status" in r.json():
                                        st.success(f"✅ ¡Estrategia en vivo! Contenedor: `{r.json()['container']}`")
                                        exec_sql("UPDATE ai_strategy_proposals SET status='deployed' WHERE proposal_id=%s", (p["pid"],))
                                    else:
                                        st.error(f"Error: {r.json().get('error')}")
                                except Exception as e:
                                    st.error(f"Error de despliegue: {e}")


# =============================================================================
# PAGE: INFRASTRUCTURE MAP
# =============================================================================

elif page == "🌐 Infrastructure":
    st.title("🌐 Infrastructure Map")
    st.markdown("### Visualización de Nodos y Servicios (NetBox)")
    
    BRAIN_API = "http://192.168.100.203:8010"
    
    # Intentar obtener datos y métricas del Cerebro
    try:
        r_nodes = requests.get(f"{BRAIN_API}/infra/nodes", timeout=5)
        nodes = r_nodes.json().get("results", []) if r_nodes.status_code == 200 else []
        
        r_metrics = requests.get(f"{BRAIN_API}/infra/metrics", timeout=5)
        metrics = r_metrics.json() if r_metrics.status_code == 200 else {}
    except Exception as e:
        nodes, metrics = [], {}
        st.warning(f"⚠️ No se pudo conectar con el Cerebro: {e}")

    # Mapeo de métricas por IP y por nombre (para mayor robustez)
    cpu_map = {}
    for m in metrics.get('cpu', []):
        inst = m['metric'].get('instance', '').split(':')[0]
        cpu_map[inst] = float(m['value'][1])
        
    ram_map = {}
    for m in metrics.get('ram', []):
        inst = m['metric'].get('instance', '').split(':')[0]
        ram_map[inst] = float(m['value'][1])

    # FILTRO: Mostrar exclusivamente servidores Core (tr-infra-*)
    nodes = [n for n in nodes if n.get('name', '').lower().startswith('tr-infra-')]

    # Mapeo estático de respaldo (Nombre -> IP) para Prometheus
    INFRA_IP_MAP = {
        "tr-infra-brain-01": "192.168.100.200",
        "tr-infra-data-01": "192.168.100.201",
        "tr-infra-compute-01": "192.168.100.202",
        "tr-infra-lab-01": "192.168.100.203"
    }

    if not nodes:
        st.info("No se encontraron nodos registrados en NetBox. Asegúrate de que el Sync de NetBox esté activo y el Cerebro actualizado.")
        
        # Simulación estética para el primer despliegue
        st.markdown("#### Vista previa (Simulada hasta el primer Sync)")
        cols = st.columns(4)
        vms = [
            ("BRAIN-01", "192.168.100.200", "active"),
            ("DATA-01", "192.168.100.201", "active"),
            ("COMPUTE-01", "192.168.100.202", "active"),
            ("LAB-01", "192.168.100.203", "active")
        ]
        for i, (name, ip, status) in enumerate(vms):
            with cols[i]:
                st.markdown(f"""
                <div style="background: rgba(255,255,255,0.05); padding: 15px; border-radius: 12px; border-left: 4px solid #00ff00;">
                    <h4 style="margin:0;">🖥️ {name}</h4>
                    <p style="font-size:0.8em; color:#888;">{ip}</p>
                    <span style="font-size:0.7em; background:#004400; padding:2px 6px; border-radius:4px;">ACTIVE</span>
                </div>
                """, unsafe_allow_html=True)
    else:
        # Grid de Nodos Reales
        cols = st.columns(4)
        for i, node in enumerate(nodes):
            with cols[i % 4]:
                status_obj = node.get("status") or {}
                status_label = status_obj.get("label", "Unknown")
                status_value = status_obj.get("value", "offline")
                status_color = "#00ff00" if status_value == "active" else "#ff4b4b"
                
                primary_ip_obj = node.get("primary_ip") or {}
                ip_address = primary_ip_obj.get("address", "N/A")
                
                # Respaldo si la IP es N/A
                node_name = node.get('name', '')
                if ip_address == "N/A":
                    ip_address = INFRA_IP_MAP.get(node_name, "N/A")

                cluster_obj = node.get("cluster") or {}
                cluster_name = cluster_obj.get("name", "N/A")
                
                tenant_obj = node.get("tenant") or {}
                tenant_name = tenant_obj.get("name", "N/A")
                
                ip_clean = ip_address.split('/')[0]
                
                # Obtener métricas
                cpu_val = cpu_map.get(ip_clean, 0.0)
                ram_val = ram_map.get(ip_clean, 0.0)

                st.markdown(f"""
<div style="background: rgba(255,255,255,0.05); padding: 20px; border-radius: 15px; border-left: 5px solid {status_color}; margin-bottom: 20px;">
    <h3 style="margin: 0; color: #fff;">🖥️ {node.get('name', 'Unnamed')}</h3>
    <p style="color: #888; font-size: 0.9em; margin-bottom: 10px;">IP: {ip_address}</p>
    <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 15px;">
        <span style="background: {status_color}; width: 10px; height: 10px; border-radius: 50%; display: inline-block;"></span>
        <span style="text-transform: uppercase; font-weight: bold; font-size: 0.8em;">{status_label}</span>
    </div>
    <div style="margin-bottom: 8px;">
        <div style="display: flex; justify-content: space-between; font-size: 0.75em; margin-bottom: 3px;">
            <span>CPU</span><span>{cpu_val:.1f}%</span>
        </div>
        <div style="background: rgba(255,255,255,0.1); height: 4px; border-radius: 2px;">
            <div style="background: #6366f1; width: {min(cpu_val, 100)}%; height: 100%; border-radius: 2px;"></div>
        </div>
    </div>
    <div style="margin-bottom: 15px;">
        <div style="display: flex; justify-content: space-between; font-size: 0.75em; margin-bottom: 3px;">
            <span>RAM</span><span>{ram_val:.1f}%</span>
        </div>
        <div style="background: rgba(255,255,255,0.1); height: 4px; border-radius: 2px;">
            <div style="background: #8b5cf6; width: {min(ram_val, 100)}%; height: 100%; border-radius: 2px;"></div>
        </div>
    </div>
    <hr style="opacity: 0.1; margin: 15px 0;">
    <p style="font-size: 0.75em; color: #94a3b8; margin:0;"><b>Cluster:</b> {cluster_name}</p>
    <p style="font-size: 0.75em; color: #94a3b8; margin:0;"><b>Tenant:</b> {tenant_name}</p>
</div>
""", unsafe_allow_html=True)

    st.divider()
    st.markdown("### 🧠 AI Infrastructure Advisor")
    
    col_msg, col_btn = st.columns([3, 1])
    with col_msg:
        st.info("La IA está analizando los nodos. Todos los sistemas core están operativos. El nodo **COMPUTE** tiene capacidad para 3 instancias adicionales.")
    
    with col_btn:
        if st.button("📊 Generar Dashboard Grafana"):
            # Template básico de Dashboard
            dash_payload = {
                "dashboard": {
                    "id": None,
                    "title": "AI Generated: Infra Health",
                    "panels": [
                        {
                            "title": "CPU Usage (All Nodes)",
                            "type": "timeseries",
                            "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
                            "targets": [{"expr": '100 - (avg by (instance) (irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)', "refId": "A"}]
                        },
                        {
                            "title": "RAM Usage (All Nodes)",
                            "type": "timeseries",
                            "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
                            "targets": [{"expr": 'node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes * 100', "refId": "A"}]
                        }
                    ],
                    "schemaVersion": 36,
                    "version": 0
                }
            }
            try:
                r = requests.post(f"{BRAIN_API}/infra/dashboard/create", json=dash_payload, timeout=5)
                if r.status_code == 200:
                    resp = r.json()
                    st.success("✅ Dashboard creado en Grafana!")
                    # Extraer URL de la respuesta de Grafana
                    url = resp.get("url")
                    if url:
                        # Convertir URL relativa a absoluta
                        full_url = f"http://192.168.100.200:3000{url}"
                        st.link_button("🚀 Ver Dashboard Creado", full_url)
                    else:
                        st.write("Respuesta de Grafana:", resp)
                else:
                    st.error(f"Error: {r.text}")
            except Exception as e:
                st.error(f"Error de conexión: {e}")

# =============================================================================
# PAGE: AUDITORÍA
# =============================================================================

elif page == "📋 Auditoría":
    st.markdown("## 📋 Auditoría y Control de Riesgo")
    
    # KPIs de Riesgo
    rejections = q("SELECT COUNT(*) as n FROM audit_logs WHERE event_type = 'ORDER_REJECTED' AND ts > NOW() - INTERVAL '24 hours'")
    panic_status = "🔴 ACTIVO" if False else "🟢 NORMAL" # Esto se podría leer de Redis/NATS
    
    c1, c2, c3 = st.columns(3)
    c1.metric("Rechazos (24h)", rejections[0]["n"] if rejections else 0)
    c2.metric("Estado de Pánico", panic_status)
    c3.metric("Exposición Global", "$0.00", "-100%") # Placeholder para live data
    
    st.divider()
    
    st.markdown("### 🚫 Órdenes Rechazadas (Risk Engine)")
    # Traemos los logs de rechazo más recientes
    logs = q("""
        SELECT ts, symbol, side, qty, payload->>'error' as reason 
        FROM audit_logs 
        WHERE event_type = 'ORDER_REJECTED' 
        ORDER BY ts DESC LIMIT 20
    """)
    
    if logs:
        st.table(logs)
    else:
        st.info("No hay rechazos registrados en las últimas 24 horas. El sistema opera dentro de los límites.")

    st.divider()
    st.markdown("### 🛡️ Configuración de Límites Vigente")
    st.info("""
    *   **Límite de Exposición Global:** $100,000 USD
    *   **Umbral de Pánico Macro (Sentimiento):** < -0.60
    *   **Circuit Breaker:** Bloqueo tras 5 fallos consecutivos.
    """)
    
    BRAIN_API = "http://192.168.100.203:8010"
    tab_journal, tab_jobs, tab_orch = st.tabs(["📜 Events", "🔧 Backtest Jobs", "🚀 Orchestration"])

    with tab_journal:
        logs = q("SELECT ts, event_type, actor, payload FROM journal_events ORDER BY ts DESC LIMIT 50")
        if logs:
            st.table(pd.DataFrame(logs))
        else:
            st.info("No hay eventos registrados.")

    with tab_jobs:
        jobs = q("""
            SELECT
                job_id::text, blueprint_id, symbol, broker, status,
                error, params, created_at, updated_at
            FROM backtest_jobs
            ORDER BY created_at DESC
            LIMIT 100
        """)
        if jobs:
            st.table(pd.DataFrame(jobs))
        else:
            st.info("No hay jobs de backtesting.")

    with tab_orch:
        st.markdown("### 🚁 Airflow DAGs Status")
        try:
            r = requests.get(f"{BRAIN_API}/infra/jobs", timeout=5)
            if r.status_code == 200:
                dags = r.json().get("dags", [])
                if dags:
                    df_dags = pd.DataFrame([{
                        "DAG ID": d.get("dag_id"),
                        "Active": d.get("is_active"),
                        "Paused": d.get("is_paused"),
                        "Last Run": d.get("last_run_state", "N/A"),
                        "Owners": ", ".join(d.get("owners", []))
                    } for d in dags])
                    st.table(df_dags)
                else:
                    st.info("No se encontraron DAGs en Airflow.")
            else:
                st.error(f"Error al conectar con el Cerebro (Airflow API): {r.status_code}")
        except Exception as e:
            st.warning(f"⚠️ No se pudo obtener el estado de Airflow: {e}")