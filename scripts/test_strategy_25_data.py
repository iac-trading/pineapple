import os
import json
import psycopg2
from datetime import datetime, timedelta, timezone
from airflow.providers.postgres.hooks.postgres import PostgresHook

def main():
    try:
        # Usamos PostgresHook para mayor seguridad/conveniencia
        hook = PostgresHook(postgres_conn_id='TRADING_DB')
        conn = hook.get_conn()
    except Exception:
        # Fallback local
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "192.168.100.201"),
            port=5432,
            dbname="trading",
            user="trading_user",
            password=os.environ["POSTGRES_PASSWORD"]
        )

    symbol = "TSLA"
    start_date = datetime(2025, 3, 1, tzinfo=timezone.utc)
    
    print(f"Injecting dummy microstructure signals for {symbol} starting {start_date}...")
    
    with conn:
        with conn.cursor() as cur:
            # Limpiar datos previos de prueba para este símbolo
            cur.execute("DELETE FROM intelligence_signals WHERE symbol=%s", (symbol,))
            
            for i in range(100):
                ts = start_date + timedelta(hours=i)
                # Simular OFI alto y Sentimiento positivo a las 00:00 UTC (para alinear con Velas Diarias de YFinance)
                is_spike = (ts.hour == 0)
                ofi = 60.0 if is_spike else 10.0
                vpin = 0.2
                sentiment = 0.5 if is_spike else 0.0
                is_toxic = False
                
                cur.execute(
                    """
                    INSERT INTO intelligence_signals (ts, symbol, ofi, vpin, sentiment, is_toxic)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (ts, symbol, ofi, vpin, sentiment, is_toxic)
                )
                
    conn.close()
    print("SUCCESS: Injected 100 test signals.")

if __name__ == "__main__":
    main()
