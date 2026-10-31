#!/usr/bin/env bash
# ./run_store.sh [dataset] [port]  -> demo store (customer side) on 0.0.0.0:9000; each shop writes to its own database
#   ./run_store.sh t10m       (shops in t10m_001..t10m_100)
set -euo pipefail
cd "$(dirname "$0")"
SET=${1:-t10m}
PORT=${2:-9000}
case "$SET" in *\**|*,*) DBS=$SET ;; *) DBS="${SET}_*" ;; esac
STORE_DBS="$DBS" exec .venv/bin/uvicorn store.app:app --host 0.0.0.0 --port "$PORT" --log-level warning
