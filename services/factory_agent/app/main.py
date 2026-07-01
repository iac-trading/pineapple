import os
import json
import asyncio
import logging
import socket
import docker
import psycopg2
import ray
from psycopg2.extras import RealDictCursor
from nats.aio.client import Client as NATS

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("FactoryAgent")

# Configuración de Entorno
# Se busca ASSIGNED_HOST (inyectado por Ansible) o el hostname real
AGENT_HOST = os.getenv("ASSIGNED_HOST", os.getenv("AGENT_HOST", socket.gethostname()))
INTERVAL = int(os.getenv("RECONCILE_INTERVAL_SEC", "10"))
DOCKER_NET = os.getenv("DOCKER_NETWORK", "platform_net")
NATS_URL = os.getenv("NATS_URL")

def get_db_conn():
    """Establece conexión con la base de datos centralizada"""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        connect_timeout=5
    )

class FactoryAgent:
    def __init__(self):
        self.dcli = docker.from_env()
        self.nc = NATS()

    def init_ray(self):
        """Inicia el clúster local de Ray y el Dashboard"""
        logger.info(f"⚡ Iniciando clúster de Ray en el puerto 8265...")
        try:
            ray.init(
                address='local',
                num_cpus=2,
                dashboard_host='0.0.0.0',
                dashboard_port=8265,
                include_dashboard=True
            )
            logger.info("✅ Dashboard de Ray activo en http://0.0.0.0:8265")
        except Exception as e:
            logger.error(f"❌ Error al iniciar Ray: {e}")

    def fetch_my_instances(self):
        """Obtiene las instancias que este host debe ejecutar"""
        conn = get_db_conn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        i.instance_id::text, i.name, i.status, i.desired_status, 
                        i.params, i.symbol, i.broker, i.qty, i.market_subject,
                        b.docker_image, b.blueprint_id
                    FROM strategy_instances i
                    JOIN strategy_blueprints b ON i.blueprint_id = b.blueprint_id
                    WHERE i.is_active = TRUE 
                      AND i.assigned_host = %s
                """, (AGENT_HOST,))
                return cur.fetchall()
        except Exception as e:
            logger.error(f"DB Error: {e}")
            return []
        finally:
            conn.close()

    def update_instance_status(self, instance_id, status, meta=None):
        """Reporta el estado real a la DB"""
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE strategy_instances 
                    SET status = %s, last_heartbeat = NOW(), meta = %s 
                    WHERE instance_id = %s::uuid
                """, (status, json.dumps(meta or {}), instance_id))
                conn.commit()
        except Exception as e:
            logger.error(f"Status update failed: {e}")
        finally:
            conn.close()

    def reconcile_instance(self, inst):
        """Asegura que el estado del contenedor coincida con la DB"""
        instance_id = inst["instance_id"]
        blueprint_id = inst["blueprint_id"]
        desired_running = (inst["desired_status"] == "running")
        image = inst["docker_image"]
        cname = f"factory_{blueprint_id}_{instance_id[:8]}" 

        env_vars = {
            "INSTANCE_ID": instance_id,
            "STRATEGY_ID": blueprint_id,
            "NATS_URL": NATS_URL,
            "SYMBOL": inst["symbol"],
            "BROKER": inst["broker"],
            "QTY": str(inst["qty"]),
            "MARKET_SUBJECT": inst["market_subject"] or "",
            "STRATEGY_PARAMS": json.dumps(inst["params"] or {})
        }

        container = None
        try:
            container = self.dcli.containers.get(cname)
        except docker.errors.NotFound:
            pass

        if desired_running:
            if not container:
                logger.info(f"🚀 Deploying {cname} from {image}")
                try:
                    self.dcli.containers.run(
                        image=image,
                        name=cname,
                        detach=True,
                        restart_policy={"Name": "unless-stopped"},
                        network=DOCKER_NET,
                        environment=env_vars,
                        mem_limit="512m",
                        cpu_quota=50000
                    )
                    self.update_instance_status(instance_id, "running", {"container": "created"})
                except Exception as e:
                    logger.error(f"Failed to start {cname}: {e}")
                    self.update_instance_status(instance_id, "error", {"error": str(e)})
            elif container.status != "running":
                logger.info(f"▶️ Starting stopped container {cname}")
                container.start()
                self.update_instance_status(instance_id, "running")
            else:
                self.update_instance_status(instance_id, "running")
        else:
            if container and container.status == "running":
                logger.info(f"🛑 Stopping {cname} (desired=stopped)")
                container.stop(timeout=10)
                self.update_instance_status(instance_id, "stopped")
            elif container:
                self.update_instance_status(instance_id, "stopped")

    async def run_loop(self):
        logger.info(f"🏭 Factory Agent V3 Started on {AGENT_HOST}")
        self.init_ray()

        if NATS_URL:
            try:
                await self.nc.connect(servers=[NATS_URL])
                logger.info("✅ Connected to NATS")
            except Exception as e:
                logger.critical(f"❌ NATS Connection CRITICAL failure: {e}")
                return # Detener el agente si no hay NATS

        while True:
            instances = self.fetch_my_instances()
            for inst in instances:
                await asyncio.to_thread(self.reconcile_instance, inst)
            await asyncio.sleep(INTERVAL)

if __name__ == "__main__":
    asyncio.run(FactoryAgent().run_loop())