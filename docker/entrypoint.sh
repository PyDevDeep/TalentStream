#!/bin/bash
set -e

echo "Waiting for database connection..."
# Extract host and user from DATABASE_URL
DB_HOST=$(echo $DATABASE_URL | sed -E 's/.*@([^:]+).*/\1/')
DB_USER=$(echo $DATABASE_URL | sed -E 's/.*:\/\/([^:]+).*/\1/')

until pg_isready -h "$DB_HOST" -U "$DB_USER"; do
  sleep 1
done
echo "Database is ready. Applying migrations..."
alembic upgrade head

# Run scheduler in background.
echo "Starting TaskIQ Scheduler..."
taskiq scheduler app.scheduler:scheduler &

# Run worker in background with a single process to conserve RAM.
echo "Starting TaskIQ Worker..."
taskiq worker app.broker:broker app.tasks --workers 1 &

# Run FastAPI on the main process so Render detects the service as live.
# Render injects $PORT (typically 10000).
echo "Starting Web API..."
exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-10000}
