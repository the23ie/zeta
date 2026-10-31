#!/usr/bin/env bash
# One Postgres database per store. ZETA follows every store database that matches the dataset prefix.
#   ./run.sh                    -> stores t1m_001..t1m_010   (1M orders, 10 stores)  on :8000
#   ./run.sh t10m 8001          -> stores t10m_001..t10m_100 (10M orders, 100 stores) on :8001
#   ZETA_HOST=0.0.0.0 ./run.sh t10m 8001   -> reachable from other laptops on the same Wi-Fi
# Keys are signed with the secret in .zeta_secret when that file exists (create: openssl rand -hex 16 > .zeta_secret)
set -euo pipefail
cd "$(dirname "$0")"
SET=${1:-t1m}
PORT=${2:-8000}
case "$SET" in *\**|*,*) DBS=$SET ;; *) DBS="${SET}_*" ;; esac
PGB=/opt/homebrew/opt/postgresql@15/bin
export LC_ALL=en_US.UTF-8   # postgres refuses to start on macOS without a valid locale
$PGB/pg_isready -q -h localhost -p 5433 || $PGB/pg_ctl -D pgdata -l pgdata/server.log -w start
if [ -f .zeta_secret ]; then export ZETA_SECRET=$(cat .zeta_secret); fi
ZETA_DBS="$DBS" exec .venv/bin/uvicorn zeta.api:app \
  --host "${ZETA_HOST:-127.0.0.1}" --port "$PORT" --log-level warning
