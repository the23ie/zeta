#!/usr/bin/env bash
# One Postgres database per store. ZETA follows every store database that matches the dataset prefix.
#   ./run.sh                    -> stores t1m_001..t1m_010   (1M orders, 10 stores)  on :8000
#   ./run.sh t10m 8001          -> stores t10m_001..t10m_100 (10M orders, 100 stores) on :8001
#   ZETA_HOST=0.0.0.0 ./run.sh t10m 8001   -> reachable from other laptops on the same Wi-Fi
#   ./run_cluster.sh t10m 2 8001           -> the same stores split over 2 ZETA nodes behind a router
# Keys are signed with the secret in .zeta_secret (created by ./setup.sh).
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
SET=${1:-t1m}
PORT=${2:-8000}
case "$SET" in *\**|*,*) DBS=$SET ;; *) DBS="${SET}_*" ;; esac
zeta_pg_up
ZETA_DBS="$DBS" exec .venv/bin/uvicorn zeta.api:app \
  --host "${ZETA_HOST:-127.0.0.1}" --port "$PORT" --log-level warning
