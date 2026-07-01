-- 02_heartbeat_trigger.sql
-- Updates strategy_instances.last_heartbeat when a new metric is inserted.

CREATE OR REPLACE FUNCTION update_strategy_heartbeat()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE strategy_instances
    SET last_heartbeat = NEW.ts,
        updated_at = now()
    WHERE instance_id = NEW.instance_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_update_heartbeat ON strategy_metrics;
CREATE TRIGGER trg_update_heartbeat
AFTER INSERT ON strategy_metrics
FOR EACH ROW
EXECUTE FUNCTION update_strategy_heartbeat();
