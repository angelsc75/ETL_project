FROM python:3.10-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && \
    apt-get install -y libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copiar archivos necesarios
COPY requirements.txt .
COPY .env .
COPY src/ src/

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Añadir el directorio actual al PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Configurar la variable de entorno para Kafka
ENV KAFKA_BOOTSTRAP_SERVERS=host.docker.internal:29092

CMD ["python", "src/main.py"]