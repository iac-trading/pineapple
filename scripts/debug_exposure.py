import os
import psycopg2
from datetime import datetime

# Configuración (Ajustar si es necesario basándose en .env)
DB_HOST = os.getenv("POSTGRES_HOST", "192.168.100.201")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "trading")
DB_USER = os.getenv("POSTGRES_USER", "tsdb")
DB_PASS = os.getenv("POSTGRES_PASSWORD")

def investigate():
    print(f"--- Investigación de Exposición de Riesgo ---")
    print(f"Conectando a {DB_HOST}:{DB_PORT} ({DB_NAME})...")
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()
        
        # 1. Ver posición neta real en la BD para R_75
        symbol = 'R_75'
        cur.execute("""
            SELECT symbol, SUM(net_position_qty) 
            FROM v_strategy_performance 
            WHERE symbol = %s 
            GROUP BY symbol
        """, (symbol,))
        row = cur.fetchone()
        net_qty = row[1] if row else 0
        
        # 2. Ver último precio de mercado
        cur.execute("""
            SELECT last FROM market_ticks 
            WHERE symbol = %s 
            ORDER BY ts DESC LIMIT 1
        """, (symbol,))
        price_row = cur.fetchone()
        last_price = price_row[0] if price_row else 0
        
        exposure = abs(net_qty) * last_price
        
        print(f"\nSímbolo: {symbol}")
        print(f"Posición Neta (BD): {net_qty}")
        print(f"Último Precio: {last_price}")
        print(f"Exposición Calculada: ${exposure:,.2f}")
        
        # 3. Ver órdenes 'filled' recientes
        print(f"\nÚltimas 5 órdenes FILLED para {symbol}:")
        cur.execute("""
            SELECT ts, side, qty, price, status 
            FROM orders 
            WHERE symbol = %s AND status = 'filled' 
            ORDER BY ts DESC LIMIT 5
        """, (symbol,))
        for r in cur.fetchall():
            print(f"  {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4]}")

        cur.close()
        conn.close()
        
        if exposure > 100000:
            print(f"\n⚠️ ALERTA: La exposición real en BD coincide con el riesgo acumulado ($1.8M?).")
        else:
            print(f"\n✅ CONCLUSIÓN: La BD dice ${exposure:,.2f}, pero el Risk Engine cree que es $1.8M.")
            print(f"Esto confirma una DESINCRONIZACIÓN en la memoria del Risk Engine.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    investigate()
