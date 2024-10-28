#!/bin/bash

# Esperar 10 segundos para asegurar que PostgreSQL esté listo
echo "Waiting for PostgreSQL to start..."
sleep 10

# Iniciar la aplicación
echo "Starting the API..."
uvicorn main:api --host 0.0.0.0 --port 8000 --reload