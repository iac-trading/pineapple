import os
import json
import asyncio
import subprocess
import logging
from nats.aio.client import Client as NATS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [DEPLOYER] %(message)s")
logger = logging.getLogger("InfraDeployer")

NATS_URL = os.getenv("NATS_URL", "nats://192.168.100.200:4222")
# Ruta absoluta al repo dentro del contenedor
REPO_PATH = "/opt/platform/repo" 

async def run_ansible_deploy(data):
    """Ejecuta el comando de Ansible para construir y desplegar"""
    strat_id = data.get("strat_id")
    logger.info(f"🏗️ Iniciando construcción de infraestructura para estrategia: {strat_id}")
    
    # IMPORTANTE: Usamos un archivo de password para el Vault en automatización
    cmd = [
        "ansible-playbook",
        "-i", "deploy/ansible/inventory/proxmox/hosts.yml",
        "deploy/ansible/playbooks/site.yml",
        "--tags", "strategies",
        "--vault-password-file", "/etc/ansible/vault_pass"
    ]
    
    try:
        # Ejecutar Ansible
        process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=REPO_PATH,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        
        if process.returncode == 0:
            logger.info(f"✅ Despliegue exitoso para {strat_id}")
        else:
            logger.error(f"❌ Error en Ansible: {stderr.decode()}")
            
    except Exception as e:
        logger.error(f"❌ Error ejecutando comando: {e}")

async def message_handler(msg):
    data = json.loads(msg.data.decode())
    # Solo disparamos si el Brain indica que se requiere un build
    if data.get("action") == "build_required":
        await run_ansible_deploy(data)

async def main():
    nc = NATS()
    await nc.connect(servers=[NATS_URL])
    # Nos suscribimos al canal que el Brain usa para notificar
    await nc.subscribe("factory.infra.deploy", cb=message_handler)
    logger.info("🚀 Listener de Infraestructura activo. Esperando señales de NATS...")
    
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())