-- Phase 19.1: Latency Audit View
-- Provides institutional-grade metrics for order execution latency

CREATE OR REPLACE VIEW v_execution_latency AS
SELECT 
    sub.instance_id,
    sub.correlation_id,
    sub.ts AS submitted_at,
    fill.ts AS filled_at,
    EXTRACT(EPOCH FROM (fill.ts - sub.ts)) * 1000 AS latency_ms,
    CASE 
        WHEN fill.event_type = 'SHADOW_FILLED' THEN 'SHADOW'
        ELSE 'LIVE'
    END AS mode
FROM 
    journal_events sub
JOIN 
    journal_events fill ON sub.correlation_id = fill.correlation_id
WHERE 
    sub.event_type = 'ORDER_SUBMITTED'
    AND fill.event_type IN ('ORDER_FILLED', 'SHADOW_FILLED');

COMMENT ON VIEW v_execution_latency IS 'Tracks the time from NATS order submission to final broker confirmation (Shadow or Live).';
