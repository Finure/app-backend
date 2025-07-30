#!/bin/sh

PGPORT=${PGPORT:-5432}

PGPASSWORD="$PGPASSWORD" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "SELECT 1;" -t >/dev/null 2>&1

if [ $? -eq 0 ]; then
  echo "Postgres connection OK"
  exit 0
else
  echo "Postgres connection FAILED"
  exit 1
fi
