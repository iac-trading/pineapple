-- FILE: deploy/ansible/files/sql/20_factory_views.sql
-- Vistas limpias (sin columnas inexistentes / sin random / sin demo)

CREATE OR REPLACE VIEW v_factory_pnl AS
SELECT
    o.instance_id,
    COALESCE(MAX(i.name), '') AS instance_name,
    COALESCE(MAX(i.symbol), '') AS symbol,
    COALESCE(MAX(i.broker), '') AS broker,
    SUM(
      CASE
        WHEN o.status='filled' AND o.side='buy'  THEN -COALESCE(o.price,0) * COALESCE(o.qty,0)
        WHEN o.status='filled' AND o.side='sell' THEN  COALESCE(o.price,0) * COALESCE(o.qty,0)
        ELSE 0
      END
    ) AS net_cashflow,
    COUNT(*) FILTER (WHERE o.status='filled') AS total_trades,
    MAX(o.ts) FILTER (WHERE o.status='filled') AS last_trade_ts
FROM orders o
JOIN strategy_instances i ON i.instance_id=o.instance_id
GROUP BY o.instance_id;

CREATE OR REPLACE VIEW v_strategy_performance AS
SELECT
    i.instance_id,
    i.name,
    i.owner,
    i.blueprint_id,
    i.assigned_host,
    i.symbol,
    i.broker,
    i.qty,
    i.status,
    i.desired_status,
    i.last_heartbeat,
    COALESCE(SUM(
      CASE
        WHEN o.status='filled' AND o.side='buy'  THEN -COALESCE(o.qty,0)
        WHEN o.status='filled' AND o.side='sell' THEN  COALESCE(o.qty,0)
        ELSE 0
      END
    ), 0) AS net_position_qty,
    COUNT(*) FILTER (WHERE o.status='filled') AS total_trades,
    COALESCE(SUM(
      CASE
        WHEN o.status='filled' AND o.side='buy'  THEN -COALESCE(o.price,0) * COALESCE(o.qty,0)
        WHEN o.status='filled' AND o.side='sell' THEN  COALESCE(o.price,0) * COALESCE(o.qty,0)
        ELSE 0
      END
    ), 0) AS net_cashflow,
    MAX(o.ts) FILTER (WHERE o.status='filled') AS last_trade_ts
FROM strategy_instances i
LEFT JOIN orders o ON o.instance_id=i.instance_id
WHERE i.is_active = TRUE
GROUP BY
    i.instance_id, i.name, i.owner, i.blueprint_id, i.assigned_host,
    i.symbol, i.broker, i.qty, i.status, i.desired_status, i.last_heartbeat;

CREATE OR REPLACE VIEW v_backtest_latest AS
SELECT
  b.job_id,
  b.status,
  b.blueprint_id,
  b.instance_id,
  b.broker,
  b.symbol,
  b.start_ts,
  b.end_ts,
  r.metrics,
  r.created_at AS result_created_at
FROM backtest_jobs b
LEFT JOIN backtest_results r ON r.job_id=b.job_id;