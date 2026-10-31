#!/usr/bin/env bash
# ./run.sh            -> ensure Postgres (port 5433) is up, serve ZETA for db "zeta" on :8000
# ./run.sh zeta10m 8001
set -euo pipefail
cd "$(dirname "$0")"
DB=${1:-zeta}
PORT=${2:-8000}
PGB=/opt/homebrew/opt/postgresql@15/bin
export LC_ALL=en_US.UTF-8   # postgres refuses to start on macOS without a valid locale
$PGB/pg_isready -q -h localhost -p 5433 || $PGB/pg_ctl -D pgdata -l pgdata/server.log -w start
ZETA_DSN="host=localhost port=5433 user=zeta dbname=$DB" exec .venv/bin/uvicorn zeta.api:app --port "$PORT" --log-level warning
