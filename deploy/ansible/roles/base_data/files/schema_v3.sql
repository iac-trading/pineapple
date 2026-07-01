-- =============================================================================
-- TRADING PLATFORM CANONICAL SCHEMA V3 (UNIFIED)
-- =============================================================================
-- This file consolidates all platform tables, views, and configurations.
-- It is designed to be idempotent and applied via Ansible.

BEGIN;

-- 0) EXTENSIONS & HELPERS
CREATE EXTENSION IF NOT EXISTS pgcrypto;
-- CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 1) CORE TABLES: BLUEPRINTS & INSTANCES
CREATE TABLE IF NOT EXISTS strategy_blueprints (
    blueprint_id    TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    docker_image    TEXT NOT NULL,
    version         TEXT DEFAULT '1.0.0',
    default_params  JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS strategy_instances (
    instance_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    blueprint_id    TEXT NOT NULL REFERENCES strategy_blueprints(blueprint_id),
    name            TEXT NOT NULL,
    owner           TEXT DEFAULT 'system',
    assigned_host   TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    broker          TEXT DEFAULT 'paper',
    qty             DOUBLE PRECISION DEFAULT 1.0,
    params          JSONB DEFAULT '{}'::jsonb,
    status          TEXT DEFAULT 'stopped',
    desired_status  TEXT DEFAULT 'stopped',
    is_active       BOOLEAN DEFAULT TRUE,
    is_shadow       BOOLEAN DEFAULT FALSE,
    last_heartbeat  TIMESTAMPTZ,
    meta            JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_instances_host_active ON strategy_instances (assigned_host, is_active);
CREATE UNIQUE INDEX IF NOT EXISTS uq_instances_host_name_active ON strategy_instances (assigned_host, name) WHERE is_active = TRUE;

DROP TRIGGER IF EXISTS trg_strategy_instances_updated_at ON strategy_instances;
CREATE TRIGGER trg_strategy_instances_updated_at BEFORE UPDATE ON strategy_instances FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- 2) MARKET DATA & AUDIT
CREATE TABLE IF NOT EXISTS market_ticks (
  ts                TIMESTAMPTZ NOT NULL,
  broker            TEXT NOT NULL,
  symbol            TEXT NOT NULL,
  bid               DOUBLE PRECISION,
  ask               DOUBLE PRECISION,
  last              DOUBLE PRECISION,
  meta              JSONB DEFAULT '{}'::jsonb
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ticks_unique ON market_ticks (ts, broker, symbol);
CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON market_ticks (broker, symbol, ts DESC);

CREATE TABLE IF NOT EXISTS market_candles (
  ts                TIMESTAMPTZ NOT NULL,
  broker            TEXT NOT NULL,
  symbol            TEXT NOT NULL,
  granularity       INTEGER NOT NULL, -- in seconds
  open              DOUBLE PRECISION NOT NULL,
  high              DOUBLE PRECISION NOT NULL,
  low               DOUBLE PRECISION NOT NULL,
  close             DOUBLE PRECISION NOT NULL,
  volume            DOUBLE PRECISION,
  meta              JSONB DEFAULT '{}'::jsonb
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_candles_unique ON market_candles (ts, broker, symbol, granularity);
CREATE INDEX IF NOT EXISTS idx_candles_symbol_ts ON market_candles (broker, symbol, granularity, ts DESC);

CREATE TABLE IF NOT EXISTS market_funding (
  ts                TIMESTAMPTZ NOT NULL,
  broker            TEXT NOT NULL,
  symbol            TEXT NOT NULL,
  rate              DOUBLE PRECISION NOT NULL,
  meta              JSONB DEFAULT '{}'::jsonb
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_funding_unique ON market_funding (ts, broker, symbol);
CREATE INDEX IF NOT EXISTS idx_funding_symbol_ts ON market_funding (broker, symbol, ts DESC);

-- -----------------------------------------------------------------------------
-- CORPORATE EVENTS (E.g. Earnings, Dividends)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS corporate_events (
  symbol            TEXT NOT NULL,
  event_date        DATE NOT NULL,
  event_type        TEXT NOT NULL, -- e.g. 'EARNINGS', 'DIVIDEND'
  time_of_day       TEXT DEFAULT 'UNKNOWN', -- e.g. 'BMO' (Before Market Open), 'AMC' (After Market Close)
  meta              JSONB DEFAULT '{}'::jsonb,
  created_at        TIMESTAMPTZ DEFAULT now(),
  updated_at        TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (symbol, event_date, event_type)
);
CREATE INDEX IF NOT EXISTS idx_corporate_events_date ON corporate_events (event_date);
CREATE INDEX IF NOT EXISTS idx_corporate_events_symbol_type ON corporate_events (symbol, event_type);

DROP TRIGGER IF EXISTS t_set_updated_at_corporate_events ON corporate_events;
CREATE TRIGGER t_set_updated_at_corporate_events BEFORE UPDATE ON corporate_events FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- -----------------------------------------------------------------------------
-- ORDERS (Journal de órdenes LIVE/PAPER/SHADOW)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    ts             TIMESTAMPTZ NOT NULL,
    correlation_id UUID NOT NULL,
    instance_id    UUID,
    broker         VARCHAR(20),
    symbol         VARCHAR(20),
    side           VARCHAR(10),
    qty            NUMERIC,
    price          NUMERIC,
    status         VARCHAR(20),
    is_shadow      BOOLEAN DEFAULT FALSE,
    broker_order_id VARCHAR(100),
    raw            JSONB,
    PRIMARY KEY (ts, correlation_id)
);
CREATE INDEX IF NOT EXISTS idx_orders_instance_ts ON orders (instance_id, ts DESC);

CREATE TABLE IF NOT EXISTS journal_events (
    ts              TIMESTAMPTZ DEFAULT now(),
    instance_id     UUID REFERENCES strategy_instances(instance_id) ON DELETE SET NULL,
    event_type      TEXT,
    actor           TEXT,
    payload         JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_journal_instance_ts ON journal_events (instance_id, ts DESC);

-- -----------------------------------------------------------------------------
-- INTELLIGENCE SIGNALS (OFI, VPIN, Sentiment persistence)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS intelligence_signals (
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    symbol      VARCHAR(20) NOT NULL,
    provider    VARCHAR(50) DEFAULT 'microstructure_nlp',
    ofi         DOUBLE PRECISION,
    vpin        DOUBLE PRECISION,
    sentiment   DOUBLE PRECISION,
    is_toxic    BOOLEAN DEFAULT FALSE,
    meta        JSONB DEFAULT '{}'::jsonb
);
SELECT create_hypertable('intelligence_signals', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_intel_sig_sym_ts ON intelligence_signals (symbol, ts DESC);

-- 3) BACKTESTING & METRICS
CREATE TABLE IF NOT EXISTS backtest_jobs (
    job_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    blueprint_id TEXT,
    instance_id  UUID,
    broker       TEXT DEFAULT 'paper',
    symbol       TEXT NOT NULL,
    start_ts     TIMESTAMPTZ,
    end_ts       TIMESTAMPTZ,
    params       JSONB DEFAULT '{}'::jsonb,
    status       TEXT NOT NULL DEFAULT 'queued',
    error        TEXT,
    created_at   TIMESTAMPTZ DEFAULT now(),
    updated_at   TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_backtest_jobs_status_created ON backtest_jobs (status, created_at);

DROP TRIGGER IF EXISTS t_set_updated_at_backtest_jobs ON backtest_jobs;
CREATE TRIGGER t_set_updated_at_backtest_jobs BEFORE UPDATE ON backtest_jobs FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS backtest_results (
    job_id        UUID PRIMARY KEY REFERENCES backtest_jobs(job_id) ON DELETE CASCADE,
    blueprint_id  TEXT,
    instance_id   UUID,
    broker        TEXT,
    symbol        TEXT NOT NULL,
    start_ts      TIMESTAMPTZ,
    end_ts        TIMESTAMPTZ,
    params        JSONB DEFAULT '{}'::jsonb,
    metrics       JSONB DEFAULT '{}'::jsonb,
    equity_curve  JSONB DEFAULT '[]'::jsonb,
    trades        JSONB DEFAULT '[]'::jsonb,
    created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS strategy_metrics (
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    instance_id UUID NOT NULL REFERENCES strategy_instances(instance_id) ON DELETE CASCADE,
    metric      TEXT NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    labels      JSONB DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_strategy_metrics_instance_ts ON strategy_metrics (instance_id, ts DESC);

-- 4) AI PROPOSALS
CREATE TABLE IF NOT EXISTS ai_strategy_proposals (
    proposal_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    goal_description TEXT,
    generated_code TEXT,
    backtest_score FLOAT,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ai_proposals_status_created ON ai_strategy_proposals (status, created_at DESC);

-- 5) SUMMARY & LEGACY COMPATIBILITY
CREATE TABLE IF NOT EXISTS strategy_summary (
    bot_id VARCHAR(50) PRIMARY KEY,
    instance_id UUID,
    total_trades INTEGER DEFAULT 0,
    win_rate DOUBLE PRECISION DEFAULT 0.0,
    sharpe_ratio DOUBLE PRECISION DEFAULT 0.0,
    max_drawdown DOUBLE PRECISION DEFAULT 0.0,
    last_update TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 6) VIEWS
CREATE OR REPLACE VIEW v_factory_pnl AS
SELECT
    o.instance_id,
    COALESCE(MAX(i.name), '') AS instance_name,
    COALESCE(MAX(i.symbol), '') AS symbol,
    COALESCE(MAX(i.broker), '') AS broker,
    SUM(CASE WHEN o.status='filled' AND o.side='buy' THEN -COALESCE(o.price,0) * COALESCE(o.qty,0) WHEN o.status='filled' AND o.side='sell' THEN COALESCE(o.price,0) * COALESCE(o.qty,0) ELSE 0 END) AS net_cashflow,
    COUNT(*) FILTER (WHERE o.status='filled') AS total_trades,
    MAX(o.ts) FILTER (WHERE o.status='filled') AS last_trade_ts
FROM orders o JOIN strategy_instances i ON i.instance_id=o.instance_id GROUP BY o.instance_id;

CREATE OR REPLACE VIEW v_strategy_performance AS
SELECT
    i.instance_id, i.name, i.owner, i.blueprint_id, i.assigned_host, i.symbol, i.broker, i.qty, i.status, i.desired_status, i.last_heartbeat,
    COALESCE(SUM(CASE WHEN o.status='filled' AND o.side='buy' THEN -COALESCE(o.qty,0) WHEN o.status='filled' AND o.side='sell' THEN COALESCE(o.qty,0) ELSE 0 END), 0) AS net_position_qty,
    COUNT(*) FILTER (WHERE o.status='filled') AS total_trades,
    COALESCE(SUM(CASE WHEN o.status='filled' AND o.side='buy' THEN -COALESCE(o.price,0) * COALESCE(o.qty,0) WHEN o.status='filled' AND o.side='sell' THEN COALESCE(o.price,0) * COALESCE(o.qty,0) ELSE 0 END), 0) AS net_cashflow,
    MAX(o.ts) FILTER (WHERE o.status='filled') AS last_trade_ts
FROM strategy_instances i LEFT JOIN orders o ON o.instance_id=i.instance_id WHERE i.is_active = TRUE
GROUP BY i.instance_id, i.name, i.owner, i.blueprint_id, i.assigned_host, i.symbol, i.broker, i.qty, i.status, i.desired_status, i.last_heartbeat;

CREATE OR REPLACE VIEW v_backtest_latest AS
SELECT b.job_id, b.status, b.blueprint_id, b.instance_id, b.broker, b.symbol, b.start_ts, b.end_ts, r.metrics, r.created_at AS result_created_at
FROM backtest_jobs b LEFT JOIN backtest_results r ON r.job_id=b.job_id;

CREATE OR REPLACE VIEW v_bot_health AS
SELECT bot_id, total_trades, last_update, NOW() - last_update AS silence_duration,
    CASE WHEN NOW() - last_update > interval '5 minutes' THEN '🔴 ALERTA: STALE' WHEN NOW() - last_update > interval '1 minute' THEN '🟡 WARNING: DELAY' ELSE '🟢 OK: ACTIVE' END AS health_status
FROM strategy_summary;

COMMIT;