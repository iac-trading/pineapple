FROM python:3.11-slim

# Instalar Ansible y dependencias de SSH
RUN apt-get update && apt-get install -y \
    ansible \
    openssh-client \
    rsync \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# El código del listener
COPY app ./app

# El contenedor necesita las llaves SSH para hablar con las VMs de Proxmox
CMD ["python", "-m", "app.main"]