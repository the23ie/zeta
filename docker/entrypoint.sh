#!/bin/sh
# Roles: load | node | router | store | keys <store ids...> | anything else is run as a command.
set -e
cd /app
case "$DATASET" in
  1m) ORDERS=1000000; STORES=10 ;;
  10m) ORDERS=10000000; STORES=100 ;;
  *) echo "DATASET must be 1m or 10m (got '$DATASET')"; exit 2 ;;
esac
PREFIX="t$DATASET"

case "${1:-node}" in
  load)
    mkdir -p /state /data
    if [ ! -s "$ZETA_SECRET_FILE" ]; then  # signs every API key; kept in the state volume
      python -c 'import secrets; print(secrets.token_hex(16))' > "$ZETA_SECRET_FILE"
      echo "created the key secret"
    fi
    have=$(python -c "
import os, psycopg2
c = psycopg2.connect(os.environ['ZETA_PG'] + ' dbname=postgres')
cur = c.cursor(); cur.execute(r\"SELECT count(*) FROM pg_database WHERE datname LIKE '${PREFIX}\_%'\")
print(cur.fetchone()[0])")
    if [ "$have" -ge "$STORES" ]; then
      echo "dataset $DATASET already loaded ($have store databases ${PREFIX}_*)"
    else
      [ -f "/data/data_$DATASET/orders.csv" ] || python generate_data.py --orders "$ORDERS" --tenants "$STORES" --out "/data/data_$DATASET"
      python split_load.py "/data/data_$DATASET" --prefix "$PREFIX"
    fi
    python -m zeta.guard 1 | sed 's/^/  /'
    echo "keys for any store: docker compose exec <zeta or router> keys <store ids>"
    ;;
  node)
    exec uvicorn zeta.api:app --host 0.0.0.0 --port 8000 --log-level warning ;;
  router)
    exec uvicorn zeta.router:app --host 0.0.0.0 --port 8000 --log-level warning ;;
  store)
    exec uvicorn store.app:app --host 0.0.0.0 --port 9000 --log-level warning ;;
  keys)
    shift; exec python -m zeta.guard "$@" ;;
  *)
    exec "$@" ;;
esac
