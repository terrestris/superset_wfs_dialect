#!/bin/bash

# OWSLib direkt aus dem Sourcecode laden
if [ -d "/app/owslib" ]; then
    echo "OWSLib directory found at /app/owslib"
    if [ -d "/app/owslib/owslib" ]; then
        echo "Using OWSLib source directly from /app/owslib"
        # The owslib package will be loaded via PYTHONPATH
    else
        echo "ERROR: /app/owslib/owslib directory not found"
        ls -la /app/owslib
        exit 1
    fi
else
    echo "ERROR: /app/owslib directory not found"
    exit 1
fi

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
export PYTHONPATH=/app/owslib:/app/pythonpath
python -m debugpy --listen 0.0.0.0:5678 -m flask run -p 8088 --host=0.0.0.0 --reload --debugger
