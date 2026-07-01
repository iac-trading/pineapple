-- Phase 19 Migration: Shadow Trading Support
-- Apply these to the live database if they don't exist

ALTER TABLE strategy_instances ADD COLUMN IF NOT EXISTS is_shadow BOOLEAN DEFAULT FALSE;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS is_shadow BOOLEAN DEFAULT FALSE;
ALTER TABLE journal_events ADD COLUMN IF NOT EXISTS correlation_id UUID;

-- Ensure indexes for performance in Shadow auditing
CREATE INDEX IF NOT EXISTS idx_instances_is_shadow ON strategy_instances (is_shadow);
CREATE INDEX IF NOT EXISTS idx_orders_is_shadow ON orders (is_shadow);
CREATE INDEX IF NOT EXISTS idx_journal_correlation ON journal_events (correlation_id);
