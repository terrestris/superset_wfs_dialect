FROM apache/superset:latest

# Installiere benötigte Python-Abhängigkeiten
USER root
RUN pip install sqlalchemy requests debugpy

# Setze Berechtigungen für /app/pythonpath und /app/owslib
COPY superset_config.py /app/pythonpath/
RUN chown -R superset:superset /app/pythonpath
RUN mkdir -p /app/owslib && chown -R superset:superset /app/owslib
COPY docker-entrypoint.sh /app/docker-entrypoint.sh

# chmod bleibt unter root!
RUN chmod +x /app/docker-entrypoint.sh

# Zurück zu superset-User
USER superset

# Superset starten via Skript
ENTRYPOINT ["/app/docker-entrypoint.sh"]
