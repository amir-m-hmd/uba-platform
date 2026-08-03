#!/bin/bash
set -e

if [ "$1" = "webserver" ] || [ "$1" = "scheduler" ] || [ "$1" = "init" ]; then
  echo "🔄 Running Airflow DB Migration..."
  airflow db migrate

  echo "👤 Creating Admin User..."
  airflow users create \
      --username admin \
      --firstname Airflow \
      --lastname Admin \
      --role Admin \
      --email admin@example.com \
      --password admin_pass_2026 || true
fi

if [ "$1" = "init" ]; then
  echo "✅ Airflow DB Migration & Admin Setup Finished Successfully."
  exit 0
fi

exec airflow "$@"