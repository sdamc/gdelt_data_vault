#!/bin/bash
set -e

echo "Airflow version: $(airflow version)"
echo "dbt version: $(dbt --version | head -n1)"

# Remove old PID file
rm -f /opt/airflow/airflow-webserver.pid

# Initialize database (suppress confirmation prompts)
echo "Initializing Airflow database..."
airflow db migrate || airflow db upgrade

# Create admin user if doesn't exist
echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin 2>/dev/null || echo "Admin user already exists"

# Start webserver and scheduler
echo "Starting Airflow webserver and scheduler..."
airflow webserver & airflow scheduler
