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

# Starte Gunicorn Webserver
superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger
