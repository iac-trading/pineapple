FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema para numpy/statsmodels
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos la estrategia y la base común
COPY app/ ./app/
COPY common/ ./common/

ENV PYTHONPATH="/app:/app/app:/app/common"

CMD ["python", "app/main.py"]
