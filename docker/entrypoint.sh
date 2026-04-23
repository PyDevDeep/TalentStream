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

# 1. Запуск TaskIQ Scheduler у фоні (оператор &)
echo "Starting TaskIQ Scheduler..."
taskiq scheduler app.scheduler:scheduler &

# 2. Запуск TaskIQ Worker у фоні (з 1 воркером для економії RAM)
echo "Starting TaskIQ Worker..."
taskiq worker app.broker:broker app.tasks --workers 1 &

# 3. Запуск FastAPI сервера на головному потоці (щоб Render бачив, що сервіс живий)
# Render автоматично прокидає змінну середовища $PORT (зазвичай 10000)
echo "Starting Web API..."
exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-10000}
