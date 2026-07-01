-- SQL para simular 20 días de velas para probar el ATR en el Risk Engine
-- Ejecutar en el nodo DATA (.201) si quieres que el test de resizing pase con éxito.

INSERT INTO market_candles (ts, broker, symbol, granularity, open, high, low, close)
SELECT 
    now() - (id || ' days')::interval,
    'binance',
    'BTC/USDT',
    86400,
    60000 + (random() * 1000), 
    62000 + (random() * 1000), 
    59000 - (random() * 1000), 
    61000 + (random() * 1000)
FROM generate_series(1, 25) AS id
ON CONFLICT (ts, broker, symbol, granularity) DO NOTHING;
