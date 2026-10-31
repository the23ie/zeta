#!/usr/bin/env bash
# One-command setup: Python environment, a Postgres cluster configured for ZETA, the key secret and the datasets.
#   ./setup.sh             everything: 1M orders / 10 stores and 10M orders / 100 stores (the 10M step takes a while)
#   ./setup.sh --small     only the 1M dataset (a few minutes)
#   ./setup.sh --locked    install the exact tested versions (requirements.lock) instead of requirements.txt
#   ZETA_PGPORT=5544 ZETA_PGDATA=pgdata2 ./setup.sh --small    a second, separate Postgres
# Needs: Python 3.11+ and PostgreSQL 15+ (macOS: brew install python postgresql@15). Safe to re-run: finished steps are skipped.
set -euo pipefail
cd "$(dirname "$0")"
SMALL=0; REQ=requirements.txt
for a in "$@"; do
  case "$a" in
    --small) SMALL=1 ;;
    --locked) REQ=requirements.lock ;;
    *) echo "unknown option $a"; exit 2 ;;
  esac
done
source ./env.sh
step() { printf '\n== %s\n' "$*"; }

step "checking tools"
PY=${PYTHON:-python3}
"$PY" -c 'import sys; assert sys.version_info >= (3, 11), sys.version' 2>/dev/null ||
  { echo "need Python 3.11+ (set PYTHON=/path/to/python3)"; exit 1; }
[ -x "$PGBIN/initdb" ] || { echo "PostgreSQL 15+ not found (macOS: brew install postgresql@15; or set PGBIN=/path/to/bin)"; exit 1; }
echo "python: $("$PY" --version)   postgres: $("$PGBIN/postgres" --version)   port: $ZETA_PGPORT   data dir: $ZETA_PGDATA"

step "python environment (.venv, $REQ)"
[ -x .venv/bin/python ] || "$PY" -m venv .venv
.venv/bin/pip install -q --upgrade pip
.venv/bin/pip install -q -r "$REQ"
echo "ok"

step "postgres cluster in $ZETA_PGDATA"
if [ ! -f "$ZETA_PGDATA/PG_VERSION" ]; then
  "$PGBIN/initdb" -D "$ZETA_PGDATA" -U zeta --auth=trust -E UTF8 --locale=en_US.UTF-8 >/dev/null
  cat >> "$ZETA_PGDATA/postgresql.conf" <<CONF

# ZETA
port = $ZETA_PGPORT
listen_addresses = 'localhost'
unix_socket_directories = '/tmp'
wal_level = logical                  # change data capture (logical decoding, test_decoding)
track_commit_timestamp = on
synchronous_commit = off
wal_writer_delay = 10ms
shared_buffers = 1GB
work_mem = 64MB
maintenance_work_mem = 1GB
max_wal_size = 8GB
checkpoint_timeout = 30min
# one database (and one logical replication stream) per store
max_connections = 1000
max_replication_slots = 256
max_wal_senders = 256
# checkpointing keeps slots between ZETA restarts; cap the WAL they may hold (ZETA falls back to a full load if exceeded)
max_slot_wal_keep_size = 20GB
CONF
  echo "created"
else
  echo "exists"
fi
zeta_pg_up >/dev/null
echo "running on port $ZETA_PGPORT"

step "key secret (.zeta_secret)"
if [ ! -f .zeta_secret ]; then
  .venv/bin/python -c 'import secrets; print(secrets.token_hex(16))' > .zeta_secret
  chmod 600 .zeta_secret
  echo "created"
else
  echo "exists"
fi
export ZETA_SECRET=$(cat .zeta_secret)

dataset() {  # dataset <dir> <prefix> <orders> <stores>
  local dir=$1 prefix=$2 orders=$3 stores=$4
  step "dataset $prefix: $orders orders, $stores stores, one database per store"
  local have
  have=$("$PGBIN/psql" -h localhost -p "$ZETA_PGPORT" -U zeta -d postgres -Atc \
    "SELECT count(*) FROM pg_database WHERE datname LIKE '${prefix}\_%'")
  if [ "$have" -ge "$stores" ]; then
    echo "databases ${prefix}_* already loaded ($have) - skipping (re-load: .venv/bin/python split_load.py $dir --prefix $prefix)"
    return
  fi
  [ -f "$dir/orders.csv" ] || .venv/bin/python generate_data.py --orders "$orders" --tenants "$stores" --out "$dir"
  .venv/bin/python split_load.py "$dir" --prefix "$prefix"
}
dataset data_1m t1m 1000000 10
[ "$SMALL" = 1 ] || dataset data_10m t10m 10000000 100

step "done"
cat <<MSG
Start ZETA:
  ./run.sh t1m 8000                   1M orders, 10 stores           -> http://localhost:8000/dashboard
  ./run.sh t10m 8001                  10M orders, 100 stores         -> http://localhost:8001/dashboard
  ./run_cluster.sh t10m 2 8001        the same 100 stores on 2 ZETA nodes behind a router
Keys (the dashboard asks for one):
  ./keys.sh 7                         owner, admin and store-7 keys
Check it:
  .venv/bin/python harness.py --dbs 't1m_*' --api http://localhost:8000 --n 1000 --mutate 500
MSG
[ "$ZETA_PGPORT" = 5433 ] || echo "(Postgres is on port $ZETA_PGPORT: run every command with ZETA_PGPORT=$ZETA_PGPORT${ZETA_PGDATA:+ ZETA_PGDATA=$ZETA_PGDATA} in front, or 'source env.sh' after exporting them.)"
