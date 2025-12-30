#!/bin/bash
set -e

# Re-install pip and drivers (fast check)
/app/.venv/bin/python -m pip install --quiet trino sqlalchemy-trino

echo "Checking if Superset is initialized..."

if ! superset fab list-users | grep -q "admin"; then
    echo "--- First time setup: Starting Initialization ---"
    
    echo "1. Upgrading Database Schema..."
    superset db upgrade
    
    echo "2. Creating Admin User..."
    superset fab create-admin \
        --username admin \
        --firstname Superset \
        --lastname Admin \
        --email admin@superset.com \
        --password admin
    
    echo "3. Initializing Roles..."
    superset init
    
    echo "--- User/Role Setup Complete ---"
else
    echo "--- Superset already initialized. Skipping User/Role setup. ---"
fi

# --- NEW COMMAND: Import Datasource from YAML ---
echo "4. Importing Trino Connection..."
superset import-datasources -p /app/datasources.yaml
# -----------------------------------------------

echo "Starting Superset Server..."
exec "$@"