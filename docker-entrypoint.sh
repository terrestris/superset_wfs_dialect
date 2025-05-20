#!/bin/bash

# Load OWSLib directly from the source code
# Not necessary as soon as a new version of OWSLib has been released
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

# Start Flask in debug mode with remote debugging support
export PYTHONPATH=/app/owslib:/app/pythonpath
python -m debugpy --listen 0.0.0.0:5678 -m flask run -p 8088 --host=0.0.0.0 --reload --debugger
