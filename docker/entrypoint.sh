set -e

echo "Waiting for database connection..."
# Extract host and user from DATABASE_URL format: postgresql+asyncpg://user:pass@host:port/dbname
DB_HOST=$(echo $DATABASE_URL | sed -E 's/.*@([^:]+).*/\1/')
DB_USER=$(echo $DATABASE_URL | sed -E 's/.*:\/\/([^:]+).*/\1/')

until pg_isready -h "$DB_HOST" -U "$DB_USER"; do
  sleep 1
done

echo "Database is ready. Applying migrations..."
alembic upgrade head

echo "Starting application..."
exec "$@"
