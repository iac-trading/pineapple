FROM python:3.10-slim

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    docker.io \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Directorio de trabajo
WORKDIR /app

# Instalar librerías de Python
RUN pip install --no-cache-dir \
    nats-py \
    psycopg2-binary \
    requests \
    fastapi \
    uvicorn

# El código se montará vía volumen para desarrollo rápido, 
# pero las dependencias ya están aquí.
CMD ["python3", "/app/hub/brain_v1.py"]
