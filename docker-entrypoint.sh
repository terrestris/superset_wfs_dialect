#!/bin/bash

# Starte Superset-Datenbankmigration
superset db upgrade

# Erstelle Admin-User, falls nicht vorhanden
superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.local \
    --password admin

# Initialisiere Superset
superset init

# Starte Flask im Debug-Modus mit Remote Debugging Support
PYTHONPATH=/app/pythonpath python -m debugpy --listen 0.0.0.0:5678 -m flask run -p 8088 --host=0.0.0.0 --reload --debugger
