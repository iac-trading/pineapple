-- FILE: deploy/ansible/files/sql/40_ai_and_backtest.sql
-- Limpio: solo IA + (opcional) índices extra. Backtest schema vive en schema_v3.sql.

BEGIN;

CREATE TABLE IF NOT EXISTS ai_strategy_proposals (
    proposal_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    goal_description TEXT,
    generated_code TEXT,
    backtest_score FLOAT,
    status TEXT DEFAULT 'pending', -- pending/approved/rejected
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Índices útiles
CREATE INDEX IF NOT EXISTS idx_ai_proposals_status_created
ON ai_strategy_proposals (status, created_at DESC);

COMMIT;