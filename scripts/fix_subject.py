import psycopg2
import os
from uuid import UUID

# Configuración desde el entorno (basado en lo que vimos en otros scripts)
DB_HOST = "192.168.100.201" # IP de tr-infra-data-01
DB_PORT = 5432
DB_NAME = "trading"
DB_USER = "tsdb"
DB_PASS = os.environ["POSTGRES_PASSWORD"]

def fix_subject():
    instance_id = "101e0000-0000-0000-0000-000000000007"
    new_subject = "md.deriv.Step_Index.tick"
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE strategy_instances 
                SET md_subject = %s 
                WHERE instance_id = %s
            """, (new_subject, instance_id))
            conn.commit()
            print(f"✅ Instance {instance_id} updated to subject: {new_subject}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    fix_subject()
