FROM python:3.10-slim

WORKDIR /app

# Instala libpq-dev para soportar la compilación de psycopg2 si se requiere
RUN apt-get update && apt-get install -y libpq-dev

COPY requirements.txt .
COPY .env .
COPY . .

RUN pip install --no-cache-dir -r requirements.txt



CMD ["python", "main.py"]