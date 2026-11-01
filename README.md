# ZETA — Z-set Exact Tenant Aggregator

Design: [ZETA_DESIGN.md](ZETA_DESIGN.md). This README tracks what is built and how to run it.

**Setting (from the PRD):** each store (tenant) has its orders, products and customers in **its own database**,
and the company owner, who owns every store, wants one dashboard across all of them. ZETA sits between the
store databases and the dashboards: it follows every store database's change stream and answers the store's
own queries and the owner's cross-store queries from memory, exactly.

```
 Store app (customers)            one Postgres database per store
   ├─ store 1 ──writes──►  t10m_001 ──┐
   ├─ store 2 ──writes──►  t10m_002 ──┤  one logical replication stream per database
   │   ...                   ...      │  (WAL → test_decoding → Z-set deltas)
   └─ store 100 ─writes──► t10m_100 ──┘
                                      ▼
                         ZETA (one process, in memory)
                           per-store Fenwick trees + mirror
                           owner = exact sum across stores
                                      ▼
            /tenants/{id}/...  (a store's own numbers, store key)
            /owner/...         (all stores, owner key) + /dashboard
```

## Layout

| Path | What |
|---|---|
| `split_load.py` | Loads the organisers' CSVs as **one Postgres database per store** (`t1m_001…t1m_010`, `t10m_001…t10m_100`) |
| `zeta/snapshot.py` | Checkpoints: each store database's state + the WAL position it reflects, written atomically to `snapshots/` |
| `zeta/cdc.py` | One `Stream` per store database (own replication slot, snapshot-exact bootstrap, freshness) and a `Hub` thread that follows all of them |
| `zeta/engine.py` | Delta engine: per-store hourly Fenwick (count/revenue × status), daily Fenwick of product-sales vectors, in-memory mirror for exact edges + join state, Z-set apply (−old, +new) |
| `zeta/owner.py` | Owner index: all-store structures (hour × store × status Fenwick, day × every-product Fenwick, one time-sorted mirror of every store's rows) kept current by the same deltas, so owner queries cost about the same for 10 or 100 stores |
| `zeta/api.py` | FastAPI: store routes, owner routes, analytics routes, `/health`, `/markers`, admin/audit, `/dashboard` |
| `zeta/router.py` | Scale-out front door: sends a store's request to the node holding it, asks every node for owner questions and adds the answers up exactly |
| `zeta/defs.py` | Metric definitions shared by the nodes and the router (ratios always recomputed from added parts) |
| `zeta/audit.py` | Snapshot-exact audit **per store database** (ledger hash + count + revenue), binary-search localization, surgical bucket rebuild |
| `zeta/guard.py` | Keys: store key bound to one store (FR-5), owner key for all stores, admin key; cost-aware token buckets (B-3) |
| `zeta/store.html` | A store owner's own dashboard (`/store`): headline numbers vs previous period plus "Ask about your store" (any metric by status, category, product, day, hour, weekday), CSV download, the API call shown; only that store's data |
| `zeta/dashboard.html` | The one dashboard: Overview (KPIs vs previous period, trend, category mix, status, top products, weekday × hour heatmap), Stores (health table, growth, sparklines, attention flags), Explore (any metric by any dimension), Operations (B-5: CDC lag, latency, per-store audit grid, repair / rebuild) |
| `store/` | Demo store app (customer side): pick a shop, place and cancel orders, written to that shop's database |
| `harness.py` | Correctness: random store checks (SQL in that store's DB) and owner checks (SQL in every store DB, combined) |
| `test_isolation.py` | Tenant isolation over every API route: wrong, forged and other stores' keys; scans responses for other stores' ids |
| `TECHNICAL_WRITEUP.md` | Full technical write-up: every claim labelled verified / reported / assumed |
| `simulate.py` | Write traffic across the store databases (inserts, status changes, old cancellations, deletes, items, backdates) |
| `demo_live.py` | Judge demo: one store's customers place and cancel orders; store + owner numbers vs SQL at every step |
| `bench.py` | Benchmarks: latency + growth 1M vs 10M (store and owner), freshness, DB safety |
| `docker-compose.yml`, `Dockerfile`, `docker/entrypoint.sh` | Docker: Postgres + loader + ZETA (single node or cluster) + optional store app |
| `setup.sh`, `requirements.txt`, `requirements.lock`, `env.sh` | One-command setup; Python dependencies (ranges / exact tested versions); shared settings (Postgres port, data dir) |
| `run.sh`, `run_cluster.sh`, `run_store.sh`, `keys.sh` | Start ZETA (one node / several nodes + router) / the store app; print keys |

## Quick start with Docker (nothing else to install)

Needs Docker Desktop (give it 8 GB of memory for the 10M dataset).

```bash
docker compose up --build -d                    # Postgres + data loader + ZETA, 1M orders / 10 stores  (~1 min)
docker compose logs loader                      # shows the owner key, admin key and store 1's key
open "http://localhost:8000/dashboard?key=<owner key>"
docker compose exec zeta keys 7                 # store 7's key -> http://localhost:8000/store?key=<it>
```

| Want | Command |
|---|---|
| 10M orders, 100 stores (main run) | `DATASET=10m docker compose up -d` |
| 2 ZETA nodes + router (scale-out), same port | `docker compose down && COMPOSE_PROFILES=cluster docker compose up -d` |
| also the demo customer store on :9000 | add `store` to the profiles, e.g. `COMPOSE_PROFILES=single,store` |
| check correctness / isolation inside Docker | `docker compose exec zeta python harness.py --api http://localhost:8000 --n 500 --mutate 300` and `... test_isolation.py --api http://localhost:8000` (`router` instead of `zeta` in cluster mode) |
| look at the data | `psql -h localhost -p 5434 -U zeta t1m_001` |
| stop / delete everything | `docker compose down` / `docker compose down -v` (also deletes data, snapshots and the key secret) |

Services: `postgres` (Postgres 15 with ZETA's settings), `loader` (runs once: key secret, data generation, one
database per store; skipped when already loaded), `zeta` (one node) or `node0` + `node1` + `router` (cluster
profile), `store` (optional). Data, snapshots and the key secret are in named volumes, so a restart resumes from
snapshots instead of reloading. Tested here (Docker Desktop, 8 GB, Apple M4):

| Run | Result |
|---|---|
| 1M, one node | first start ~1 min including data; 600/600 checks match SQL, 246 isolation checks, 0 failures |
| 1M, 2 nodes + router | 600/600 and 246/0 through the router; store app up |
| 10M, one node | data generated + loaded into 100 databases in 3 min; first ZETA load 23 s, restart from snapshots 2.9 s; 600/600 checks (100 owner checks across 100 databases), 246/0 isolation; ZETA uses 3 GB, Postgres 1 GB |

## Setup without Docker (one command)

Needs Python 3.11+ and PostgreSQL 15+ (macOS: `brew install python postgresql@15`). Then:

```bash
./setup.sh             # venv + packages, a Postgres cluster for ZETA, the key secret, 1M and 10M datasets
./setup.sh --small     # only the 1M dataset (about 30 s)
./setup.sh --locked    # exact tested package versions (requirements.lock)
ZETA_PGPORT=5544 ./setup.sh --small    # use another port if 5433 is taken (then prefix the run commands the same way)
```

Safe to re-run: finished steps are skipped. Tested on a clean copy of the repo (no venv, no data, no secret, Postgres on
5544): `--small` finished in 24 s, then a 2-node cluster on it passed 600/600 correctness and 239/239 isolation checks.

What it sets up (on this laptop it already exists):

- Dedicated Postgres cluster in `./pgdata` on **port 5433**, `wal_level=logical`, `max_replication_slots/max_wal_senders = 256`,
  `max_connections = 1000` (your normal Postgres on 5432 is untouched)
- Store databases: `t1m_001…t1m_010` (1M orders, 10 stores) and `t10m_001…t10m_100` (10M orders, 100 stores, skewed:
  store 1 = 2.3M orders, store 100 = 15.6k), loaded with
  `python split_load.py data_1m --prefix t1m` and `python split_load.py data_10m --prefix t10m`
- `.zeta_secret` signs all keys (`./keys.sh 1 2` prints store keys plus the owner and admin keys)
- Python venv in `.venv` (`requirements.txt`)

## Run

```bash
./run.sh                          # 10 store databases (1M) on :8000
ZETA_HOST=0.0.0.0 ./run.sh t10m 8001   # 100 store databases (10M) on :8001, reachable on the Wi-Fi
./run_cluster.sh t10m 2 8001      # or: the same 100 stores on 2 ZETA nodes behind a router on :8001
./run_store.sh t10m               # optional: customer store app on :9000
```

- Owner dashboard: `http://localhost:8001/dashboard?key=<owner key>`: every store, plus the Operations tab
  (`/monitor` redirects there). Operations → **Store keys** shows any store's key and dashboard link, and rotates keys.
- Store dashboard: `http://localhost:8001/store?key=<store key>`: a store owner sees only their own store.
  A store key that opens `/dashboard` is sent to `/store`.

Store keys (the company owner hands them out):

```bash
./keys.sh 7 12            # keys for stores 7 and 12 (plus the owner and admin keys)
./keys.sh --all t10m      # every store's key
./keys.sh --rotate 7      # new key for store 7; the old one stops working at once, even on a running server
```

Or over the API with the owner key: `GET /admin/keys?stores=7` and `POST /admin/keys/rotate?tenant=7`. Keys are HMACs of
the store id and a per-store version (in `.zeta_key_versions.json`, not in git) signed with `.zeta_secret`; rotating
bumps the version. To send a store owner a link that works from their laptop, open the owner dashboard through
this laptop's Wi-Fi address (not `localhost`) before copying it.
- Customer store: `http://localhost:9000/?shop=7`

API (`from`/`to` ISO-8601, naive = UTC, range `[from, to)`, trend buckets UTC):

```bash
K=$(./keys.sh 1 | awk '/tenant/{print $3}')     # store 1's key
O=$(./keys.sh | awk '/owner/{print $2}')        # owner key
curl -H "X-API-Key: $K" "localhost:8001/tenants/1/orders/count?from=2026-10-24&to=2026-10-31"
curl -H "X-API-Key: $K" "localhost:8001/tenants/1/orders/by-status?from=2026-10-24"
curl -H "X-API-Key: $K" "localhost:8001/tenants/1/orders/trend?from=2026-10-01&granularity=day"
curl -H "X-API-Key: $K" "localhost:8001/tenants/1/products/top?from=2026-10-24&n=10"
curl -H "X-API-Key: $O" "localhost:8001/owner/orders/count?from=2026-10-24"
curl -H "X-API-Key: $O" "localhost:8001/owner/orders/by-store?from=2026-10-24"
curl -H "X-API-Key: $O" "localhost:8001/owner/orders/by-status?from=2026-10-24&stores=1,2,3"
curl -H "X-API-Key: $O" "localhost:8001/owner/orders/trend?from=2026-10-01&granularity=day"
curl -H "X-API-Key: $O" "localhost:8001/owner/products/top?from=2026-10-24&n=10"
curl -H "X-API-Key: $O" "localhost:8001/analytics/summary?from=2026-10-24&to=2026-10-31"            # KPIs + previous period + category mix
curl -H "X-API-Key: $O" "localhost:8001/analytics/stores?from=2026-10-24&to=2026-10-31"             # store health
curl -H "X-API-Key: $O" "localhost:8001/analytics/explore?metric=aov&by=weekday&from=2026-10-01&to=2026-10-31"
curl -H "X-API-Key: $O" "localhost:8001/analytics/heatmap?from=2026-10-01&to=2026-10-31"
```

Explore metrics: `orders, revenue, net_sales, aov, cancel_rate, return_rate, sales`; groupings: `none, store, status,
category, product, day, hour_of_day, weekday` (category / product use `sales`). A store key may call the cross-store
and analytics routes too, but only ever gets its own store.

Correctness and demo:

```bash
.venv/bin/python harness.py --dbs 't1m_*' --n 1000 --mutate 2000
.venv/bin/python test_isolation.py --api http://localhost:8001     # 246 isolation checks, exit code 0 = pass
.venv/bin/python harness.py --dbs 't10m_*' --api http://localhost:8001 --n 1000 --owner-n 100 --mutate 2000
.venv/bin/python demo_live.py                    # store 5 on the 10M server, plus owner totals
.venv/bin/python bench.py latency | freshness | safety
```

### Restarts and snapshots (checkpointing)

ZETA saves each store database's state with the WAL position (LSN) it reflects: every 60 s (only databases that
changed), on `POST /admin/snapshot`, and on a clean shutdown. Replication slots are kept, and Postgres is only told
"confirmed" for positions that are saved, so it keeps all WAL ZETA might still need. On restart ZETA loads each
snapshot, resumes the slot, skips transactions already in the snapshot and replays the rest; the owner index is then
rebuilt from the restored stores. A missing, damaged or unusable snapshot makes only that database fall back to a full load.

| Restart (10M orders, 100 store databases) | Time to answering queries |
|---|---|
| full load (no snapshots) | 22–60 s |
| clean restart (shutdown snapshot) | **1.8 s** |
| crash (`kill -9`) after 1,000 unsaved transactions | **2.5 s**, then 750/750 checks match SQL, audit green |
| one damaged snapshot | 2.3 s (99 resumed, 1 fully loaded) |

Duplicate safety: with confirmations held back (test switch `ZETA_TEST_HOLD_CONFIRM=1`), Postgres resent 600
transactions after a crash; ZETA skipped the 300 already in the snapshot, applied the other 300, and 800/800 checks matched.
Only databases with new data are written: audit markers and heartbeats don't count (after 200 orders touching
19 stores, 19 of 100 databases were saved, 0.5 s; with no new data nothing is written and the position is still confirmed).

**Heartbeat:** every 5 s ZETA writes a tiny timestamped marker into each store database's WAL and times its trip
back through the change stream, so the CDC lag is always live (median 7.8 ms, worst 15 ms at 10M) even when no
orders arrive, and it keeps every slot's confirmed position moving (at most ~4.5 MB of WAL held back).

Settings: `ZETA_SNAPSHOT_INTERVAL` (s, default 60), `ZETA_SNAPSHOTS=0` to disable, `ZETA_SNAPSHOT_DIR`,
`ZETA_HEARTBEAT` (s, default 5, 0 = off).
Postgres caps the WAL kept for slots at 20 GB (`max_slot_wal_keep_size`); beyond that ZETA does a full load.

Reload store databases: stop ZETA, then `.venv/bin/python split_load.py data_10m --prefix t10m`.

### Scale-out: several ZETA nodes behind a router

One ZETA process keeps every store in memory on one machine. To grow past that, `./run_cluster.sh t10m 2 8001` starts:

- **Nodes** (`:8011`, `:8012`, ...): ordinary ZETA servers, each following only its share of the store databases
  (`ZETA_SHARD=k/N`: store database number mod N, so the few big stores land on different nodes). Each node has its
  own change streams, snapshots, audit and heartbeat for its stores. Node 0 holds 5.8M orders, node 1 4.2M.
- **Router** (`:8001`): the one address dashboards and API users talk to; it holds no data. A store's request
  (`/tenants/7/...`, or anything called with store 7's key) goes to the node holding store 7. An owner's request goes
  to every node at once (only the nodes holding the stores asked for with `stores=`), and the answers are added up:
  counts and revenue are summed, trends merged bucket by bucket, top-N lists merged and re-ranked, and ratios
  (average order value, cancel and return rate) recomputed from summed numerators and denominators, never averaged.
  Operations (audit, repair, rebuild, snapshots, keys, health) are sent to every node and the results combined.

Keys, dashboards and API calls are unchanged: the same URLs work against the router. Verified on 10M / 100 stores, 2 nodes:
720/720 store and owner checks match SQL after 300 live writes; 239 isolation checks, 0 failures; every dashboard
route (summary, store health, explore ratios by day/weekday/hour, heatmap, categories, top products) equals the sum of
the two nodes' own answers (22/22); corrupt → audit → repair → rebuild work through the router. Owner queries through
the router take 3–6 ms (p50) against 2–4 ms on one node; both nodes restart from snapshots in about 1.3 s.

### New stores while ZETA runs

Every 15 s (`ZETA_DISCOVER_INTERVAL`) each node looks for new store databases matching its pattern and its share.
A new one is picked up on the second sighting (so a database still being created is not caught half-way): full load
from an exported snapshot, its own replication slot, then the owner index is rebuilt. `POST /admin/discover` (owner
key) does it at once. The router learns new stores when it meets an unknown store id, and every 30 s.
Tested: created `t10m_101` with 40 orders while the cluster ran; it answered through the router with its own key 23 s
later, and 480/480 checks across all 101 databases matched SQL.

## Results — one database per store

### Correctness (`harness.py`)

| Run | Checks | Match |
|---|---|---|
| 1M, 10 store DBs, after 2000 live writes | 1000 store + 300 owner (incl. sales by category) | **1300/1300** |
| 10M, 100 store DBs, after 2000 live writes | 1000 store + 150 owner (incl. sales by category) | **1150/1150** |
| 10M, after rebuilding store 7's database during live writes | 400 store + 60 owner | **460/460** |

- Audit runs per store database at that database's MVCC snapshot (100 snapshots, ~12 s per full sweep on 10M), green throughout
- Corrupt demo (store 3, 10M): audit red → binary search found the exact hour (17 Sep 13:00) with 15 range queries → rebuilt from the mirror → green
- `POST /admin/rebuild?tenant=7` rebuilt only `t10m_007` in 1.2 s while the other 99 stores kept streaming
- `demo_live.py` (store 5): each order / cancel visible in the store's numbers and in the owner's all-store total in 8–20 ms; both matched SQL (the owner total against SQL run in all 100 databases) at every step

### Speed + Growth (`bench.py latency`, with the owner index)

Store rows = the biggest store (374k orders at 1M, 2.3M at 10M). OWNER rows = all stores (10 databases at 1M, 100 at 10M).
SQL for OWNER rows = the same question run in every store database and added up (what the owner does without ZETA).

| Query | ZETA p50 1M | ZETA p50 10M | ZETA p99 10M | Growth | SQL p50 1M | SQL p50 10M |
|---|---|---|---|---|---|---|
| count 7d | 0.96 ms | 1.40 ms | 2.52 ms | 1.46× | 4.2 ms | 23.5 ms |
| count 90d | 0.90 ms | 1.04 ms | 1.86 ms | 1.16× | 18.9 ms | 140 ms |
| by-status 30d | 0.90 ms | 1.13 ms | 6.32 ms | 1.26× | 19.1 ms | 146 ms |
| trend hour 3d | 1.44 ms | 1.75 ms | 3.06 ms | 1.22× | 4.1 ms | 39 ms |
| trend day 90d | 1.62 ms | 1.93 ms | 4.15 ms | 1.19× | 101 ms | 596 ms |
| top10 30d | 1.09 ms | 1.56 ms | 2.52 ms | 1.44× | 58 ms | 416 ms |
| top10 90d | 1.10 ms | 1.57 ms | 2.65 ms | 1.43× | 139 ms | 722 ms |
| OWNER count 7d | 1.38 ms | 1.69 ms | 2.48 ms | 1.22× | 91 ms | 768 ms |
| OWNER by-store 30d | 1.19 ms | 2.91 ms | 6.46 ms | 2.44× | 101 ms | 1,164 ms |
| OWNER trend day 90d | 1.71 ms | 2.03 ms | 2.30 ms | 1.19× | 289 ms | 3,215 ms |
| OWNER top10 30d | 1.58 ms | 6.12 ms | 8.54 ms | 3.88× | 311 ms | 4,736 ms |

Before the owner index, owner queries grew 2.8–6.3× (each answer added up every store's structures). Now:
count and trend are flat; by-store returns 10× more rows (one per store); top-N stays exact by reading every item
sold in the two partial days at the edges, so its edge cost follows orders per day (next step: hourly per-product
subtotals).

### Freshness (`bench.py freshness`): order committed in `t10m_001` → visible in ZETA

| | p50 | p99 |
|---|---|---|
| idle | 9.6 ms | 18.7 ms |
| under 300 write tx/s across all stores | 11.9 ms | 27.3 ms |

### DB safety (`bench.py safety`, 100 store DBs, 4 app workers, 4 dashboard users × 10 refresh/s, 70 s each)

Half the dashboard refreshes are a store's own numbers, half are the owner's all-store numbers. ZETA (CDC + its
60 s audit) runs in all three scenarios.

| Scenario | App p50 | App p99 | Δp50 | Δp99 | Postgres CPU | Dashboard q/s served |
|---|---|---|---|---|---|---|
| no dashboards | 0.43 ms | 6.03 ms | — | — | 1.6 cores | 0 |
| dashboards on ZETA | 0.39 ms | 5.34 ms | −9% | −11% (noise) | 1.3 cores | 40.5 (all offered) |
| dashboards on direct SQL | 1.00 ms | 14.04 ms | +133% | +133% | 4.1 cores | 1.1 (saturated) |

Direct SQL for an owner dashboard also needs a connection to every store database per user (100 per user here),
which exhausted Postgres's default connection limit; ZETA needs one replication connection per store database in total.
