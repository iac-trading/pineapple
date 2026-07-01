-- 1. Crear la tabla de resumen que espera el Collector
CREATE TABLE IF NOT EXISTS strategy_summary (
    bot_id VARCHAR(50) PRIMARY KEY,
    instance_id UUID,
    total_trades INTEGER DEFAULT 0,
    win_rate DOUBLE PRECISION DEFAULT 0.0,
    sharpe_ratio DOUBLE PRECISION DEFAULT 0.0,
    max_drawdown DOUBLE PRECISION DEFAULT 0.0,
    last_update TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. (Opcional) Si quieres que el Collector use la tabla vieja, 
-- tendríamos que cambiar la lógica de INSERT, pero es mejor tener un resumen limpio.