# docker/grafana/Dockerfile
FROM grafana/grafana:latest

# Configurar variables de entorno
ENV GF_SECURITY_ADMIN_PASSWORD=admin
ENV GF_USERS_ALLOW_SIGN_UP=false

# Copiar configuraciones personalizadas si las tienes
# COPY ./docker/grafana/provisioning /etc/grafana/provisioning
# COPY ./docker/grafana/dashboards /var/lib/grafana/dashboards

USER grafana