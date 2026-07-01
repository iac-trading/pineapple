from airflow.providers.postgres.hooks.postgres import PostgresHook

def main():
    hook = PostgresHook(postgres_conn_id="TRADING_DB")
    conn = hook.get_conn()
    cur = conn.cursor()
    
    cur.execute("SELECT MIN(ts), MAX(ts), COUNT(*) FROM market_candles WHERE symbol='TSLA'")
    row = cur.fetchone()
    print("TSLA Candles count/range:", row)
    
    cur.execute("SELECT ts, broker, symbol, granularity, close FROM market_candles WHERE symbol='TSLA' LIMIT 3")
    rows = cur.fetchall()
    print("\nSample TSLA rows:")
    for r in rows:
        print(r)
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
