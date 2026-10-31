# ZETA — Z-set Exact Tenant Aggregator

Design: [ZETA_DESIGN.md](ZETA_DESIGN.md). This README tracks what is built and how to run it.

## Layout

| Path | What |
|---|---|
| `zeta/engine.py` | Delta engine: per-tenant hourly Fenwick (count/revenue × status), daily Fenwick of product-sales vectors, in-memory mirror for exact edges + join state, Z-set apply (−old, +new) |
| `zeta/cdc.py` | Snapshot-exact bootstrap (`CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT` → `SET TRANSACTION SNAPSHOT`) and the logical replication stream (`test_decoding`, built into Postgres) |
| `zeta/api.py` | FastAPI: the 4 FR-7 endpoints + `/health`, admin/audit endpoints, `/monitor`; every response carries freshness (`as_of_lsn`, `cdc_lag_ms`) |
| `zeta/audit.py` | Snapshot-exact audit (ledger hash + count + revenue per tenant), binary-search localization, surgical bucket rebuild |
| `zeta/guard.py` | API keys bound to one tenant (FR-5) and cost-aware per-tenant token buckets by tier (B-3) |
| `zeta/monitor.html` | `/monitor` page (B-5): CDC lag, latency p50/p99, per-tenant audit grid, demo buttons |
| `bench.py` | Benchmarks: latency + growth 1M vs 10M, freshness, DB safety |
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

Every tenant route needs a key bound to that tenant (`.venv/bin/python -m zeta.guard 1 2` prints keys for tenants 1, 2 and the admin key):

```bash
K=$(.venv/bin/python -c "from zeta.guard import tenant_key; print(tenant_key(1))")
curl -H "X-API-Key: $K" "localhost:8000/tenants/1/orders/count?from=2026-10-01T10:15:30Z&to=2026-10-20T03:00:01Z"
curl -H "X-API-Key: $K" "localhost:8000/tenants/1/orders/by-status?from=2026-10-01&to=2026-10-08"
curl -H "X-API-Key: $K" "localhost:8000/tenants/1/orders/trend?from=2026-10-25&to=2026-10-31&granularity=hour"
curl -H "X-API-Key: $K" "localhost:8000/tenants/1/products/top?from=2026-10-01&to=2026-10-31&n=10"
curl "localhost:8000/health"
```

Monitor: open `http://localhost:8001/monitor?key=<admin key>` (the server also logs the URL at startup).
Set `ZETA_AUTH=0` / `ZETA_LIMITS=0` to turn keys / limits off, `ZETA_AUDIT_INTERVAL` (seconds, default 60, 0 = off).

`from`/`to`: ISO-8601, naive = UTC, range is `[from, to)`. Trend buckets are UTC.

Correctness:

```bash
.venv/bin/python harness.py --n 1000                    # read-only
.venv/bin/python harness.py --n 1000 --mutate 3000      # after 3000 live write transactions
.venv/bin/python harness.py --dsn "host=localhost port=5433 user=zeta dbname=zeta10m" --api http://localhost:8001 --n 1000 --mutate 3000
```

Live judge demo (one tenant's customers place orders, cancel some; latency + SQL match at every step):

```bash
.venv/bin/python demo_live.py                          # tenant 5 on the 10M server
.venv/bin/python demo_live.py --tenant 1 --orders 3 --cancel 2
```

Sample run (10M): commit → visible in ZETA's API in 5.6–24.2 ms (median 16 ms) across 7 changes; count,
revenue and top-3 products matched direct SQL after every step; a cancel retracted the product's sales to the
exact previous value; tenant 2 unchanged and tenant 5's key rejected on tenant 2 (403); demo orders deleted at the end.

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

## Checkpoint 2 — Sun 1 Nov, 10 am (P1 + P2, built early)

### Speed + Growth (`bench.py latency`, tenant 1: 374k orders at 1M, 2.3M at 10M)

| Query | ZETA p50 1M | ZETA p50 10M | ZETA p99 10M | Growth | SQL p50 1M | SQL p50 10M |
|---|---|---|---|---|---|---|
| count 7d | 0.54 ms | 0.54 ms | 0.65 ms | 1.02× | 2.0 ms | 8.7 ms |
| count 90d | 0.53 ms | 0.53 ms | 0.60 ms | 1.00× | 19.9 ms | 184 ms |
| by-status 30d | 0.55 ms | 0.55 ms | 0.75 ms | 1.00× | 8.2 ms | 45 ms |
| trend hour 3d | 0.82 ms | 0.82 ms | 0.91 ms | 1.00× | 2.0 ms | 12 ms |
| trend day 90d | 0.94 ms | 1.10 ms | 1.15 ms | 1.17× | 53 ms | 326 ms |
| top10 30d | 0.64 ms | 0.86 ms | 0.99 ms | 1.34× | 47 ms | 677 ms |
| top10 90d | 0.64 ms | 0.85 ms | 0.90 ms | 1.31× | 77 ms | 884 ms |

HTTP round trip from a client on the same laptop; engine time alone is 0.03–0.3 ms.

### Freshness (`bench.py freshness`, 10M): order committed → visible in ZETA

| | p50 | p99 |
|---|---|---|
| idle | 7.8 ms | 17.4 ms |
| under 300 write tx/s | 11.6 ms | 25.3 ms |

### DB safety (`bench.py safety`, 10M, 4 app workers, 8 dashboard users × 5 refresh/s offered)

| Scenario | App p50 | App p99 | Δp50 | Δp99 | Postgres CPU | Dashboard q/s served |
|---|---|---|---|---|---|---|
| no dashboards | 0.28 ms | 1.93 ms | — | — | ~0.5 cores | 0 |
| dashboards on ZETA | 0.28 ms | 1.74 ms | −1% | −10% (noise) | ~0.2 cores | 42 (all offered) |
| dashboards on direct SQL | 0.63 ms | 12.18 ms | +124% | +532% | 6.9 cores | 7 (saturated) |

The periodic audit costs one ~3.5 s aggregate query per minute on 10M (md5 over every row), ≈ 0.6% of this 10-core machine.

### Correctness, audit, rebuild

- Harness after the P1/P2 changes, using per-tenant keys: 1000/1000 on 1M and 1000/1000 on 10M, both after 2000 live writes; audit green on both
- Snapshot-exact audit while 400 tx/s stream in: green every time, ~130 newer transactions subtracted per audit
- Corrupt demo (10M, tenant 1): audit red → binary search finds the exact hour with 15 range queries → bucket rebuilt from mirror → green. Also run from the `/monitor` buttons
- Full rebuild (`POST /admin/rebuild`) during live writes: 2.1 s on 1M, harness still 1000/1000 afterwards

### Isolation + limits

- No key / forged key → 401; another tenant's key → 403
- Cost known before running: 1-hour count = 1.2 units, 90-day top-N with wide edges = 274 units
- Tenant 2 bursting 400 wide top-N queries: 163 served, 237 rejected with `429 + Retry-After`; tenant 3 unaffected

### Still open (P2)

- B-4 `metrics.yaml` validator (reject non-invertible aggregates)
- `?as_of=` time travel (the per-xid delta log used by the audit is the building block)
- docker-compose packaging (Docker daemon not running on this laptop)
