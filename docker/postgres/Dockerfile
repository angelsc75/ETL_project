# docker/postgres/Dockerfile
FROM postgres:13

# Copiar archivo de inicialización
COPY ./docker/postgres/init/init.sql /docker-entrypoint-initdb.d/

# Configurar locale (opcional, para soporte de caracteres especiales)
RUN localedef -i es_ES -c -f UTF-8 -A /usr/share/locale/locale.alias es_ES.UTF-8
ENV LANG es_ES.utf8