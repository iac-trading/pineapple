FROM python:3.10-slim

# Evitar que Python genere archivos .pyc y forzar logs en tiempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Instalar dependencias del sistema mínimas para psycopg2
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Instalar librerías necesarias de forma permanente
RUN pip install --no-cache-dir nats-py psycopg2-binary

# El script se montará vía volumen para facilitar actualizaciones sin re-build
CMD ["python", "/opt/platform/scripts/metrics_collector.py"]