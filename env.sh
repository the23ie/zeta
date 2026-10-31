# Shared settings for every script (sourced, not run). Override any of them in the environment.
#   ZETA_PGPORT=5544 ./run.sh      -> use a Postgres on port 5544
export ZETA_PGPORT=${ZETA_PGPORT:-5433}
export ZETA_PGDATA=${ZETA_PGDATA:-pgdata}
export ZETA_PG=${ZETA_PG:-"host=localhost port=$ZETA_PGPORT user=zeta"}
export STORE_PG=${STORE_PG:-$ZETA_PG}
if [ -z "${PGBIN:-}" ]; then
  for d in /opt/homebrew/opt/postgresql@{15,16,17}/bin /usr/local/opt/postgresql@{15,16,17}/bin /usr/lib/postgresql/{15,16,17}/bin; do
    [ -x "$d/pg_ctl" ] && PGBIN=$d && break
  done
  PGBIN=${PGBIN:-$(dirname "$(command -v pg_ctl 2>/dev/null || echo /usr/bin/pg_ctl)")}
fi
export PGBIN
export LC_ALL=${LC_ALL:-en_US.UTF-8}   # postgres refuses to start on macOS without a valid locale
if [ -f .zeta_secret ]; then export ZETA_SECRET=$(cat .zeta_secret); fi

zeta_pg_up() {   # start the project's Postgres if it is not running
  "$PGBIN/pg_isready" -q -h localhost -p "$ZETA_PGPORT" ||
    "$PGBIN/pg_ctl" -D "$ZETA_PGDATA" -l "$ZETA_PGDATA/server.log" -o "-p $ZETA_PGPORT" -w start
}
