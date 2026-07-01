import os
import json
import logging
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from hmmlearn.hmm import GaussianHMM
from sklearn.preprocessing import StandardScaler

# Configuration
DB = {
    "host": os.getenv("POSTGRES_HOST", "192.168.100.201"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "trading"),
    "user": os.getenv("POSTGRES_USER", "tsdb"),
    "password": os.environ["POSTGRES_PASSWORD"],
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("RegimeMaster")

def fetch_series(cur, symbol, broker, days=1000):
    query = """
        SELECT ts, close as val
        FROM market_candles
        WHERE symbol = %s AND broker = %s
        AND ts > NOW() - INTERVAL '%s days'
        ORDER BY ts ASC
    """
    cur.execute(query, (symbol, broker, days))
    df = pd.DataFrame(cur.fetchall())
    if not df.empty:
        df['ts'] = pd.to_datetime(df['ts'])
        df.set_index('ts', inplace=True)
    return df

import asyncio
from nats.aio.client import Client as NATS

async def publish_regime(regime_label, state_id, meta):
    nats_url = os.environ["NATS_URL"]
    nc = NATS()
    try:
        await nc.connect(servers=[nats_url])
        payload = {
            "regime_label": regime_label,
            "regime_state": state_id,
            "timestamp": datetime.now().isoformat(),
            "meta": meta
        }
        await nc.publish("intelligence.regime", json.dumps(payload).encode())
        
        # Estrategia 3: Señales de Orquestación (pause/resume)
        if regime_label == "BEARISH":
            orch_payload = {"action": "pause", "target": "trend_following", "reason": "High Volatility / Bearish Regime"}
            await nc.publish("factory.orchestration", json.dumps(orch_payload).encode())
            logger.warning("🚨 Sent PAUSE signal to Trend Followers")
        elif regime_label == "BULLISH":
            orch_payload = {"action": "resume", "target": "all", "reason": "Bullish Regime detected"}
            await nc.publish("factory.orchestration", json.dumps(orch_payload).encode())
            logger.info("✅ Sent RESUME signal to all bots")

        logger.info(f"Published regime change to NATS: {regime_label}")
        await nc.flush()
        await nc.close()
    except Exception as e:
        logger.error(f"Failed to publish to NATS: {e}")

async def run_analysis():
    logger.info("Starting Regime Analysis...")
    
    try:
        conn = psycopg2.connect(**DB)
        cur = conn.cursor(cursor_factory=RealDictCursor)
    except Exception as e:
        logger.error(f"Failed to connect to DB: {e}")
        return

    # 1. Fetch Data
    logger.info("Fetching macro signals (SPY, VIX, T10Y2Y, HG=F, GC=F)...")
    
    # Ensure table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_regimes (
            ts TIMESTAMP PRIMARY KEY,
            regime_state INTEGER,
            regime_label TEXT,
            confidence FLOAT,
            meta JSONB
        );
    """)
    conn.commit()

    spy = fetch_series(cur, "SPY", "yfinance")
    vix = fetch_series(cur, "^VIX", "yfinance")
    yield_curve = fetch_series(cur, "T10Y2Y", "fred")
    copper = fetch_series(cur, "HG=F", "yfinance")
    gold = fetch_series(cur, "GC=F", "yfinance")

    missing = []
    if spy.empty: missing.append("SPY (yfinance)")
    if vix.empty: missing.append("^VIX (yfinance)")
    if yield_curve.empty: missing.append("T10Y2Y (fred)")
    if copper.empty: missing.append("HG=F (yfinance)")
    if gold.empty: missing.append("GC=F (yfinance)")

    if missing:
        logger.error(f"Missing critical data series: {', '.join(missing)}")
        logger.info("Tip: Run 'python3 scripts/data_loader_v3.py --symbol <SYMBOL> --source <SOURCE>' to populate the database.")
        if "T10Y2Y (fred)" in missing:
            logger.warning("FRED_API_KEY must be set in your environment to fetch T10Y2Y.")
        return

    # 2. Preprocessing & Feature Selection
    # Optional components
    components = {
        'vix': vix,
        'yield_curve': yield_curve,
        'copper': copper,
        'gold': gold
    }

    # Normalize timestamps to date only for better joining across sources
    for name, data in components.items():
        if not data.empty:
            data.index = data.index.normalize()
    spy.index = spy.index.normalize()

    # Start with a base DataFrame from the most reliable source (SPY)
    df = spy.rename(columns={'val': 'spy'})
    logger.info(f"Base data (SPY): {len(df)} rows")
    
    # Optional components join
    for name, data in components.items():
        if not data.empty:
            before_count = len(df)
            # Use inner join but normalize timestamps first
            df = df.join(data.rename(columns={'val': name}), how='inner')
            logger.info(f"Joined {name}: {before_count} -> {len(df)} rows")
    
    if df.empty:
        logger.error("Join resulted in empty dataset. Check if timestamps align (e.g. holidays or timezones).")
        return

    # Feature Engineering
    df['spy_ret'] = np.log(df['spy'] / df['spy'].shift(1))
    
    available_features = ['spy_ret']
    
    if 'vix' in df.columns:
        available_features.append('vix')
    
    if 'yield_curve' in df.columns:
        available_features.append('yield_curve')
        
    if 'copper' in df.columns and 'gold' in df.columns:
        df['cg_ratio'] = df['copper'] / df['gold']
        available_features.append('cg_ratio')
    
    df.dropna(inplace=True)

    if len(available_features) < 2:
        logger.error(f"Not enough features to train HMM. Need at least 2. Available: {available_features}")
        return

    logger.info(f"Final training set: {len(df)} rows. Features: {available_features}")
    X = df[available_features].values
    
    # Scale features for HMM
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 3. HMM Model
    n_states = 3
    logger.info(f"Training HMM model ({n_states} states)...")
    model = GaussianHMM(n_components=n_states, covariance_type="full", n_iter=1000, random_state=42)
    model.fit(X_scaled)
    
    states = model.predict(X_scaled)
    df['state'] = states
    
    # 4. Labeling States (Deterministic Mapping)
    # 0: BULLISH (Max SPY returns)
    # 1: VOLATILE/NEUTRAL
    # 2: BEARISH (Min SPY returns)
    
    state_means = df.groupby('state')['spy_ret'].mean()
    orig_bull_state = state_means.idxmax()
    orig_bear_state = state_means.idxmin()
    orig_neutral_state = [s for s in range(n_states) if s not in [orig_bull_state, orig_bear_state]][0]
    
    # Create mapping from model state to our standardized state
    mapping = {
        orig_bull_state: 0,
        orig_neutral_state: 1,
        orig_bear_state: 2
    }
    labels = {0: "BULLISH", 1: "VOLATILE/NEUTRAL", 2: "BEARISH"}
    
    # Apply mapping to the entire series
    df['std_state'] = df['state'].map(mapping)
    current_std_state = int(df['std_state'].iloc[-1])
    current_label = labels[current_std_state]
    current_ts = df.index[-1]
    
    # Transition Matrix (Standardized)
    # We re-order the model.transmat_ to match our 0,1,2 mapping
    raw_transmat = model.transmat_
    std_transmat = np.zeros_like(raw_transmat)
    
    inv_mapping = {v: k for k, v in mapping.items()}
    for i in range(n_states):
        for j in range(n_states):
            std_transmat[i, j] = raw_transmat[inv_mapping[i], inv_mapping[j]]
    
    logger.info(f"Analysis Complete. Current Regime: {current_label} (Standardized State {current_std_state})")
    logger.info(f"Transition Matrix (Standardized):\n{std_transmat}")

    # 5. Persistence
    meta = {
        "features": available_features,
        "state_means_spy": {labels[mapping[k]]: float(v) for k, v in state_means.to_dict().items()},
        "transition_matrix": std_transmat.tolist(),
        "next_state_probs": std_transmat[current_std_state].tolist(),
        "data_points": int(len(df)),
        "last_update": datetime.now().isoformat()
    }
    
    if 'vix' in df.columns:
        state_vix_dict = df.groupby('state')['vix'].mean().to_dict()
        meta["state_vix"] = {labels[mapping[k]]: float(v) for k, v in state_vix_dict.items()}
    
    try:
        cur.execute("""
            INSERT INTO market_regimes (ts, regime_state, regime_label, confidence, meta)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ts) DO UPDATE SET 
                regime_state = EXCLUDED.regime_state,
                regime_label = EXCLUDED.regime_label,
                meta = EXCLUDED.meta
        """, (current_ts, current_std_state, current_label, 1.0, json.dumps(meta)))
        conn.commit()
        logger.info(f"Regime saved to DB for {current_ts}")
        
        # Notify via NATS
        await publish_regime(current_label, current_std_state, meta)
        
    except Exception as e:
        logger.error(f"Failed to save results: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    asyncio.run(run_analysis())
