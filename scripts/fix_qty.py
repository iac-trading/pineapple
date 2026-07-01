import os
import psycopg2

# Configuración
DB_HOST = os.getenv("POSTGRES_HOST", "192.168.100.201")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "trading")
DB_USER = os.getenv("POSTGRES_USER", "tsdb")
DB_PASS = os.getenv("POSTGRES_PASSWORD")

def update_qty():
    instance_id = "101e0000-0000-0000-0000-000000000006"
    new_qty = 0.01
    
    print(f"Buscando instancia {instance_id}...")
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
            user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()
        
        # 1. Update qty and params
        cur.execute("""
            UPDATE strategy_instances 
            SET qty = %s 
            WHERE instance_id = %s
        """, (new_qty, instance_id))
        
        if cur.rowcount > 0:
            print(f"✅ ÉXITO: Qty actualizada a {new_qty} para {instance_id}.")
            conn.commit()
        else:
            # Quizás el ID es parcial o diferente en la BD
            print(f"⚠️ ADVERTENCIA: No se encontró la instancia {instance_id} para actualizar.")
            
            # Intentar buscar por nombre o patrón si falló
            cur.execute("SELECT instance_id, name, symbol, qty FROM strategy_instances WHERE instance_id::text LIKE '101e%'")
            rows = cur.fetchall()
            if rows:
                print("Instancias similares encontradas:")
                for r in rows:
                    print(f"  {r[0]} | {r[1]} | {r[2]} | Qty actual: {r[3]}")
            
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    update_qty()
