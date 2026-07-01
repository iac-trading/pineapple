-- FILE: deploy/ansible/files/sql/10_base_schema.sql
-- CANON V3: Base schema (SIN datos demo)

CREATE EXTENSION IF NOT EXISTS pgcrypto;
-- CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS strategy_blueprints (
    blueprint_id    TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    docker_image    TEXT NOT NULL,
    version         TEXT DEFAULT '1.0.0',
    default_params  JSONB,
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
    params          JSONB,

    status          TEXT DEFAULT 'stopped',
    desired_status  TEXT DEFAULT 'stopped',
    is_active       BOOLEAN DEFAULT TRUE,
    is_shadow       BOOLEAN DEFAULT FALSE,

    last_heartbeat  TIMESTAMPTZ,
    meta            JSONB,

    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_instances_host_active
ON strategy_instances (assigned_host, is_active);

DROP TRIGGER IF EXISTS trg_strategy_instances_updated_at ON strategy_instances;
CREATE TRIGGER trg_strategy_instances_updated_at
BEFORE UPDATE ON strategy_instances
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS orders (
    ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
    order_id        UUID DEFAULT gen_random_uuid(),
    instance_id     UUID NOT NULL REFERENCES strategy_instances(instance_id),
    correlation_id  TEXT,
    broker          TEXT,
    symbol          TEXT,
    side            TEXT,
    qty             DOUBLE PRECISION,
    price           DOUBLE PRECISION,
    status          TEXT,
    is_shadow       BOOLEAN DEFAULT FALSE,
    broker_order_id TEXT,
    raw             JSONB
);

CREATE TABLE IF NOT EXISTS journal_events (
    ts              TIMESTAMPTZ DEFAULT now(),
    instance_id     UUID,
    correlation_id  UUID,
    event_type      TEXT,
    actor           TEXT,
    payload         JSONB
);