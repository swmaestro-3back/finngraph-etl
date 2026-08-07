set -e

for migration in /migrations/versions/*.sql; do
  if [ -f "$migration" ]; then
    echo "running migration $migration"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "$migration"
  fi
done
