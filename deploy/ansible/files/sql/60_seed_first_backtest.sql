-- =============================================================================
-- REGISTRO INICIAL — Blueprint + Instancia para Backtesting
-- Estrategia: volatility_breakout_v1 (R_75, Deriv)
-- Ejecutar en: nodo DATA (.201) dentro del contenedor TimescaleDB
-- =============================================================================

BEGIN;

-- 1) Registrar el Blueprint (la definición del software)
INSERT INTO strategy_blueprints (
    blueprint_id,
    name,
    docker_image,
    version,
    default_params
) VALUES (
    'volatility_breakout_v1',
    'Volatility Breakout — R_75 Donchian + ATR',
    'factory/strat_101:latest',
    '1.0.0',
    '{
        "entry_p":   20,
        "exit_p":    10,
        "atr_p":     14,
        "atr_min":   0.3,
        "stop_mult": 1.5,
        "qty":       1.0
    }'::jsonb
)
ON CONFLICT (blueprint_id) DO UPDATE SET
    name           = EXCLUDED.name,
    docker_image   = EXCLUDED.docker_image,
    version        = EXCLUDED.version,
    default_params = EXCLUDED.default_params;

-- 2) Registrar la Instancia de Backtesting (broker=paper)
--    UUID fijo para reproducibilidad en scripts y tests
INSERT INTO strategy_instances (
    instance_id,
    blueprint_id,
    name,
    owner,
    assigned_host,
    symbol,
    broker,
    qty,
    params,
    status,
    desired_status,
    is_active
) VALUES (
    '101e0000-0000-0000-0000-000000000101'::uuid,
    'volatility_breakout_v1',
    'VB_R75_paper_bt',
    'system',
    'tr-infra-lab-01',          -- nodo LAB (.203) es el de backtesting
    'R_75',
    'paper',
    1.0,
    '{
        "entry_p":   20,
        "exit_p":    10,
        "atr_p":     14,
        "atr_min":   0.3,
        "stop_mult": 1.5,
        "qty":       1.0
    }'::jsonb,
    'stopped',
    'stopped',
    true
)
ON CONFLICT (instance_id) DO UPDATE SET
    blueprint_id   = EXCLUDED.blueprint_id,
    name           = EXCLUDED.name,
    assigned_host  = EXCLUDED.assigned_host,
    symbol         = EXCLUDED.symbol,
    broker         = EXCLUDED.broker,
    params         = EXCLUDED.params;

-- 3) Insertar el primer Job de Backtest (30 días de R_75)
--    Cuando el backtest_worker lo detecte (status='queued'), lo publicará
--    como bt.request en NATS y el backtester lo procesará.
INSERT INTO backtest_jobs (
    blueprint_id,
    instance_id,
    broker,
    symbol,
    start_ts,
    end_ts,
    params,
    status
) VALUES (
    'volatility_breakout_v1',
    '101e0000-0000-0000-0000-000000000101'::uuid,
    'deriv',
    'R_75',
    NOW() - INTERVAL '30 days',
    NOW(),
    '{
        "entry_p":        20,
        "exit_p":         10,
        "atr_p":          14,
        "atr_min":        0.3,
        "stop_mult":      1.5,
        "qty":            1.0,
        "slippage_pct":   0.0001
    }'::jsonb,
    'queued'
)
-- No insertar duplicado si ya existe un job pendiente para este blueprint
ON CONFLICT DO NOTHING;

COMMIT;

-- Verificación
SELECT
    j.job_id,
    j.blueprint_id,
    j.symbol,
    j.status,
    j.start_ts,
    j.end_ts,
    b.name AS blueprint_name,
    b.version
FROM backtest_jobs j
JOIN strategy_blueprints b ON b.blueprint_id = j.blueprint_id
WHERE j.blueprint_id = 'volatility_breakout_v1'
ORDER BY j.created_at DESC
LIMIT 5;
