FROM python:3.10-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && \
    apt-get install -y libpq-dev librdkafka-dev && \
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

# Variables de entorno por defecto
ENV KAFKA_HOST=kafka
ENV KAFKA_PORT=29092
ENV REDIS_HOST=redis
ENV REDIS_PORT=6379

CMD ["python", "src/main.py"]