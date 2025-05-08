FROM apache/superset:latest

# Installiere benötigte Python-Abhängigkeiten
USER root
RUN pip install sqlalchemy requests

COPY superset_config.py /app/pythonpath/
RUN chown -R superset:superset /app/pythonpath/superset_config.py
COPY docker-entrypoint.sh /app/docker-entrypoint.sh

# chmod bleibt unter root!
RUN chmod +x /app/docker-entrypoint.sh

# Zurück zu superset-User
USER superset

# Superset starten via Skript
ENTRYPOINT ["/app/docker-entrypoint.sh"]