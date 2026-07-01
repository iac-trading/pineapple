import psycopg2
import logging
import asyncio
from pathlib import Path
from nats.aio.client import Client as NATS

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [AI-BRAIN] %(message)s")
logger = logging.getLogger("AI-Brain")

# Rutas y Configuración de Infraestructura
STRATEGIES_PATH = Path("/opt/platform/repo/strategies")
DB_PARAMS = {
    "host": os.getenv("POSTGRES_HOST", "192.168.100.207"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "platform"),
    "user": os.getenv("POSTGRES_USER", "platform"),
    "password": os.getenv("POSTGRES_PASSWORD")
}
# Apuntamos al nodo STR-01/02 que es tu clúster NATS
NATS_URL = os.getenv("NATS_URL", "nats://192.168.100.200:4222")

def get_db_conn():
    return psycopg2.connect(**DB_PARAMS)

async def notify_infrastructure(strat_id, name):
    """Notifica al Deployer para que dispare Ansible"""
    nc = NATS()
    try:
        # Reintentos para NATS en caso de micro-cortes
        await nc.connect(servers=[NATS_URL], connect_timeout=5)
        payload = {
            "strat_id": strat_id, 
            "name": name, 
            "action": "build_required",
            "timestamp": time.time()
        }
        await nc.publish("factory.infra.deploy", json.dumps(payload).encode())
        logger.info(f"📣 Notificación enviada para Estrategia {strat_id}")
        await nc.drain()
    except Exception as e:
        logger.error(f"❌ Error NATS (¿Está activo el servidor en .200?): {e}")
    finally:
        await nc.close()

def write_strategy_files(strat_id, name, logic):
    """Escribe los archivos físicos. Corregido para evitar carpetas con 'NAME:'"""
    # Limpieza de nombres para el sistema de archivos
    safe_name = name.strip().replace(" ", "_").lower()
    folder_name = f"{strat_id}_{safe_name}"
    base_path = STRATEGIES_PATH / folder_name
    
    try:
        # Aseguramos la ruta física
        (base_path / "app").mkdir(parents=True, exist_ok=True)
        
        # 1. Dockerfile
        dockerfile = (
            "FROM python:3.11-slim\n"
            "WORKDIR /app\n"
            "ENV PYTHONUNBUFFERED=1\n"
            "COPY requirements.txt .\n"
            "RUN pip install --no-cache-dir -r requirements.txt\n"
            "COPY app ./app\n"
            "CMD [\"python\", \"-m\", \"app.main\"]"
        )
        with open(base_path / "Dockerfile", "w") as f: f.write(dockerfile)
        
        # 2. requirements.txt
        reqs = "nats-py\npandas\npsycopg2-binary\nnumpy\npydantic"
        with open(base_path / "requirements.txt", "w") as f: f.write(reqs)
        
        # 3. app/main.py
        main_py = (
            f"import os, json, asyncio, logging\n"
            f"# Lógica: {logic}\n\n"
            f"async def main():\n"
            f"    print('🚀 Estrategia {strat_id} ({name}) iniciando...')\n"
            f"    # Próximo paso: Integrar el motor de ejecución NATS\n\n"
            f"if __name__ == '__main__':\n"
            f"    asyncio.run(main())"
        )
        with open(base_path / "app" / "main.py", "w") as f: f.write(main_py)
        
        return True
    except Exception as e:
        logger.error(f"❌ Error de escritura en disco: {e}")
        return False

async def brain_worker():
    """Vigila la tabla ai_strategy_proposals"""
    logger.info(f"🧠 AI-Brain activo. Conectando a NATS en {NATS_URL}...")
    
    while True:
        conn = None
        try:
            conn = get_db_conn()
            with conn.cursor() as cur:
                # Buscamos propuestas pendientes
                cur.execute("""
                    SELECT proposal_id, goal_description 
                    FROM ai_strategy_proposals 
                    WHERE status = 'pending' 
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                """)
                row = cur.fetchone()
                
                if row:
                    pid, raw_desc = row
                    logger.info(f"⚡ Procesando propuesta {pid}")
                    
                    try:
                        # PARSING CORREGIDO: Usamos split(":", 1) para evitar el error 'NAME'
                        data = {}
                        for item in raw_desc.split("|"):
                            key, val = item.split(":", 1)
                            data[key.strip()] = val.strip()
                        
                        # Ejecutamos la creación de archivos
                        if write_strategy_files(data['ID'], data['NAME'], data['PROMPT']):
                            cur.execute("UPDATE ai_strategy_proposals SET status = 'generated' WHERE proposal_id = %s", (pid,))
                            conn.commit()
                            # Notificamos a la infraestructura
                            await notify_infrastructure(data['ID'], data['NAME'])
                    except Exception as parse_err:
                        logger.error(f"❌ Error de parsing en prompt: {parse_err}")
                        cur.execute("UPDATE ai_strategy_proposals SET status = 'error' WHERE proposal_id = %s", (pid,))
                        conn.commit()
                        
        except Exception as e:
            logger.error(f"❌ Error en el loop principal: {e}")
        finally:
            if conn: conn.close()
        
        await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(brain_worker())