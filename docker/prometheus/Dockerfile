# docker/prometheus/Dockerfile
FROM prom/prometheus:latest

# Copiar archivo de configuración
COPY ./docker/prometheus/prometheus.yml /etc/prometheus/prometheus.yml


# Configurar permisos
USER root
RUN chown -R nobody:nobody /etc/prometheus /prometheus
USER nobody