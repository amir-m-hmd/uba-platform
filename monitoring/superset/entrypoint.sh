#!/bin/bash
set -e

echo "==========================================="
echo "Waiting for services..."
echo "==========================================="

echo "Upgrading Superset metadata..."
superset db upgrade

echo "Creating admin user..."

superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@example.com \
    --password admin_pass_2026 || true

echo "Initializing Superset..."

superset init

echo "Starting Superset..."

exec gunicorn \
    --bind 0.0.0.0:8088 \
    --workers 2 \
    --timeout 120 \
    "superset.app:create_app()"