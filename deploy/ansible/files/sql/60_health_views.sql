-- 60 | Axio-Quant: Vista de Monitoreo de Salud
-- Esta vista calcula el tiempo de silencio basado en strategy_summary
CREATE OR REPLACE VIEW v_bot_health AS
SELECT 
    bot_id,
    total_trades,
    last_update,
    NOW() - last_update AS silence_duration,
    CASE 
        WHEN NOW() - last_update > interval '5 minutes' THEN '🔴 ALERTA: STALE'
        WHEN NOW() - last_update > interval '1 minute' THEN '🟡 WARNING: DELAY'
        ELSE '🟢 OK: ACTIVE'
    END AS health_status
FROM strategy_summary; --