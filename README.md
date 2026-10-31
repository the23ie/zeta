# ZETA — Z-set Exact Tenant Aggregator

Design: [ZETA_DESIGN.md](ZETA_DESIGN.md). This README tracks what is built and how to run it.

## Layout

| Path | What |
|---|---|
| `zeta/engine.py` | Delta engine: per-tenant hourly Fenwick (count/revenue × status), daily Fenwick of product-sales vectors, in-memory mirror for exact edges + join state, Z-set apply (−old, +new) |
| `zeta/cdc.py` | Snapshot-exact bootstrap (`CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT` → `SET TRANSACTION SNAPSHOT`) and the logical replication stream (`test_decoding`, built into Postgres) |
| `zeta/api.py` | FastAPI: the 4 FR-7 endpoints + `/health`, every response carries freshness (`as_of_lsn`, `cdc_lag_ms`) |
| `harness.py` | Correctness harness: N random (tenant, range, query) checks vs direct SQL, optionally after a burst of writes |
| `simulate.py` | Store-app write traffic (inserts, status changes, old-order cancellations, deletes, item add/remove, backdated `created_at`) |
| `load.sh` | Loads a generated dataset (replaces `load.sql`, whose `\copy … :'var'` does not interpolate in psql 15) |
| `run.sh` | Starts Postgres if needed and serves ZETA |

## Setup (already done on this laptop)

- Dedicated Postgres 15 cluster in `./pgdata` on **port 5433**, `wal_level=logical` (your normal Postgres on 5432 is untouched)
- Databases: `zeta` = 1M orders / 10 tenants, `zeta10m` = 10M orders / 100 tenants (both generated with `--end 2026-10-31T09:00:00`)
- Python venv in `.venv`

## Run

```bash
./run.sh                 # 1M db on :8000
./run.sh zeta10m 8001    # 10M db on :8001
```

```bash
curl "localhost:8000/tenants/1/orders/count?from=2026-10-01T10:15:30Z&to=2026-10-20T03:00:01Z"
curl "localhost:8000/tenants/1/orders/by-status?from=2026-10-01&to=2026-10-08"
curl "localhost:8000/tenants/1/orders/trend?from=2026-10-25&to=2026-10-31&granularity=hour"
curl "localhost:8000/tenants/1/products/top?from=2026-10-01&to=2026-10-31&n=10"
curl "localhost:8000/health"
```

`from`/`to`: ISO-8601, naive = UTC, range is `[from, to)`. Trend buckets are UTC.

Correctness:

```bash
.venv/bin/python harness.py --n 1000                    # read-only
.venv/bin/python harness.py --n 1000 --mutate 3000      # after 3000 live write transactions
.venv/bin/python harness.py --dsn "host=localhost port=5433 user=zeta dbname=zeta10m" --api http://localhost:8001 --n 1000 --mutate 3000
```

Reset a database: stop its ZETA server, then `./load.sh data_1m zeta`.

## Checkpoint 1 — Sat 31 Oct, 5 pm (P0 done)

| Run | Checks | Match | Engine p50 (count / top / trend-hour) | API p50 via HTTP | Catch-up after writes |
|---|---|---|---|---|---|
| 1M, read-only | 1000 | **1000** | 0.043 / 0.135 / 0.317 ms | 0.5–1.9 ms | — |
| 1M, after 3000 writes | 1000 | **1000** | 0.035 / 0.097 / 0.292 ms | 0.5–1.6 ms | 25 ms |
| 10M, after 3000 writes | 1000 | **1000** | 0.052 / 0.135 / 0.361 ms | 0.5–2.0 ms | 29 ms |

- Bootstrap: 1M in 2.1 s, 10M orders + 16M items in 12.3 s
- Growth 1M → 10M: engine latency ≈ 1.2–1.5× (cost depends on time buckets, not rows)
- Writes covered: new orders, late orders with past µs timestamps, status changes on recent and 60-day-old orders, deletes, items added/removed on existing orders, `created_at` moved (including outside the bucket axis)

Implementation notes vs the design doc:
- `test_decoding` instead of `wal2json` (ships with Postgres, nothing to install; same information: old row via `REPLICA IDENTITY FULL`, xids, commit timestamps)
- Mirror is in-memory numpy (sorted columns + CSR items) instead of SQLite: edges are a `searchsorted` + `bincount`
- Native Postgres instead of docker-compose (Docker daemon was not running)

## Checkpoint 2 — Sun 1 Nov, 10 am (P1 + P2)

1. Ledger hash per tenant and per bucket + snapshot-exact audit against the SQL hash (`/audit`)
2. Benchmark script: latency 1M vs 10M (biggest tenant, full range), freshness (insert → visible), app-query latency with and without dashboard load
3. B-3 cost-aware per-tenant token bucket + API keys bound to tenant (FR-5)
4. B-5 `/monitor` page: p50/p99, CDC lag, audit status per tenant
5. Surgical rebuild demo: corrupt a bucket → audit red → hash search finds the hour → rebuild → green
6. B-4 `metrics.yaml` validator, `?as_of=` time travel (if time allows)
