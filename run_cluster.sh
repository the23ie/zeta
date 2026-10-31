#!/usr/bin/env bash
# Scale-out: N ZETA nodes, each following every N-th store database, behind one router.
#   ./run_cluster.sh t10m 2 8001     -> nodes on :8011 and :8012 (50 stores each), router on :8001
#   ./run_cluster.sh t10m 4 8001     -> 4 nodes on :8011..:8014
# Dashboards, keys and API calls stay the same: use the router's port. Stop with Ctrl-C (stops everything).
# Do not run ./run.sh on the same dataset at the same time: each store database has one replication slot.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
SET=${1:-t10m}
N=${2:-2}
PORT=${3:-8001}
BASE=${ZETA_NODE_PORT:-8011}
case "$SET" in *\**|*,*) DBS=$SET ;; *) DBS="${SET}_*" ;; esac
zeta_pg_up
mkdir -p logs
pids=()
trap 'kill ${pids[*]:-} 2>/dev/null; wait' EXIT
trap 'exit 130' INT TERM
NODES=""
for ((k = 0; k < N; k++)); do
  p=$((BASE + k))
  ZETA_DBS="$DBS" ZETA_SHARD="$k/$N" .venv/bin/uvicorn zeta.api:app --host 127.0.0.1 --port "$p" \
    --log-level warning > "logs/node$k.log" 2>&1 &
  pids+=($!)
  NODES="${NODES:+$NODES,}http://127.0.0.1:$p"
  echo "node $k/$N on :$p (log: logs/node$k.log)"
done
echo "router on :$PORT -> $NODES"
ZETA_NODES="$NODES" .venv/bin/uvicorn zeta.router:app --host "${ZETA_HOST:-127.0.0.1}" --port "$PORT" \
  --log-level warning &
pids+=($!)
while :; do  # (macOS bash 3.2 has no `wait -n`)
  for p in "${pids[@]}"; do
    kill -0 "$p" 2>/dev/null || { echo "a process exited (see logs/); stopping the cluster"; exit 1; }
  done
  sleep 2
done
