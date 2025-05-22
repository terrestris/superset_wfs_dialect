#!/bin/bash

# Start Superset database migration
superset db upgrade

# Create admin user if not available
superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.local \
    --password admin

# Initialize Superset
superset init

# Set Python path
export PYTHONPATH=/app/pythonpath

# Start Flask based on DEBUG_MODE
if [ "${DEBUG_MODE:-false}" = "true" ]; then
    echo "Starting in debug mode..."
    python -m debugpy --listen 0.0.0.0:5678 -m flask run -p 8088 --host=0.0.0.0 --reload --debugger
else
    echo "Starting in production mode..."
    gunicorn --bind "0.0.0.0:8088" \
        --timeout 120 \
        --workers 10 \
        --limit-request-line 0 \
        --limit-request-field_size 0 \
        "superset.app:create_app()"
fi
