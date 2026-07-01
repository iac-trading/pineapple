-- =============================================================================
-- FILE: deploy/ansible/roles/base_data/files/add_market_candles.sql
-- Migración: agrega tabla market_candles al schema v3
-- Ejecutar UNA sola vez directamente en TimescaleDB (DATA .201)
-- Es 100% idempotente — seguro volver a ejecutar
-- =============================================================================

BEGIN;

-- ============================================================
-- market_candles — OHLC por símbolo y granularidad
-- Patrón idéntico a market_ticks del schema_v3.sql
-- ============================================================
CREATE TABLE IF NOT EXISTS market_candles (
    ts          TIMESTAMPTZ       NOT NULL,
    broker      TEXT              NOT NULL,
    symbol      TEXT              NOT NULL,
    granularity INTEGER           NOT NULL,   -- 300=5m  3600=1h  86400=1d
    open        DOUBLE PRECISION  NOT NULL,
    high        DOUBLE PRECISION  NOT NULL,
    low         DOUBLE PRECISION  NOT NULL,
    close       DOUBLE PRECISION  NOT NULL,
    meta        JSONB             DEFAULT '{}'::jsonb,

    PRIMARY KEY (ts, broker, symbol, granularity)
);

-- Índice principal — igual que idx_ticks_symbol_ts en market_ticks
CREATE INDEX IF NOT EXISTS idx_candles_symbol_gran_ts
ON market_candles (broker, symbol, granularity, ts DESC);

-- ============================================================
-- Vista de cobertura — útil para Airflow DAG y Factory UI
-- ============================================================
CREATE OR REPLACE VIEW v_candles_coverage AS
SELECT
    symbol,
    granularity,
    CASE granularity
        WHEN 300   THEN '5m'
        WHEN 3600  THEN '1h'
        WHEN 86400 THEN '1d'
        ELSE granularity::text || 's'
    END                                                     AS gran_label,
    COUNT(*)                                                AS total_candles,
    MIN(ts)::date                                           AS desde,
    MAX(ts)::date                                           AS hasta,
    ROUND(EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts))) / 86400.0, 1) AS span_dias,
    MAX(ts)                                                 AS ultimo_update
FROM market_candles
GROUP BY symbol, granularity
ORDER BY symbol, granularity;

COMMIT;

-- Verificar que quedó bien
SELECT 'market_candles creada OK' AS status;
SELECT * FROM v_candles_coverage;