#!/usr/bin/env bash
# Usage: ./load.sh data_1m [dbname]
# (load.sql's `\copy ... FROM :'var'` does not interpolate in psql 15, so we drive \copy from here.)
set -euo pipefail
cd "$(dirname "$0")"
DIR=${1:?usage: load.sh <data_dir> [db]}
DB=${2:-zeta}
PSQL="/opt/homebrew/opt/postgresql@15/bin/psql -q -v ON_ERROR_STOP=1 -h localhost -p 5433 -U zeta"
# a database with a replication slot cannot be dropped: remove ZETA's (inactive) slots first
$PSQL postgres -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = '$DB' AND NOT active" >/dev/null
$PSQL postgres -c "DROP DATABASE IF EXISTS $DB" -c "CREATE DATABASE $DB"
$PSQL "$DB" -f schema.sql
for t in tenants customers products orders order_items; do
  echo "loading $t"
  $PSQL "$DB" -c "\\copy $t FROM '$DIR/$t.csv' CSV HEADER"
done
$PSQL "$DB" -c "ANALYZE"
echo "loaded $DIR into $DB"
