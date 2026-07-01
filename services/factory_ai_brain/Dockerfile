FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Dependencias para que la IA gestione código y archivos
RUN apt-get update && apt-get install -y \
    rsync \
    git \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# El entrypoint asume que usaremos un script de Python para el servidor MCP
CMD ["python", "-m", "app.main"]