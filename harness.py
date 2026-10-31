#!/usr/bin/env python3
"""Correctness harness: random checks of the ZETA API against direct SQL on the store databases.

Every store lives in its own Postgres database.
  * store checks: random (store, range, query) -> SQL in that store's database
  * owner checks: the cross-store owner routes -> SQL run in every included store database, then combined

  python harness.py --dbs 't1m_*' --n 1000                    # read-only checks
  python harness.py --dbs 't1m_*' --n 1000 --mutate 2000      # first apply 2000 write transactions
  python harness.py --dbs 't10m_*' --api http://localhost:8001 --n 1000 --owner-n 100

Before checking, the same transactional marker is written into every store database and we wait
until ZETA has applied it on every stream, so both sides answer from the same committed state.
"""
import argparse
import random
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from datetime import timedelta, timezone

import httpx
import psycopg2

from zeta.cdc import discover
from zeta.guard import admin_key, owner_key, tenant_key

UTC = timezone.utc
PG = "host=localhost port=5433 user=zeta"
STATUSES = ["placed", "shipped", "delivered", "cancelled", "returned"]
TOP_SQL = ("SELECT i.product_id, sum(i.qty::bigint * i.price_cents) s FROM order_items i "
           "JOIN orders o ON o.order_id = i.order_id "
           "WHERE o.tenant_id = %s AND o.created_at >= %s AND o.created_at < %s "
           "AND o.status NOT IN ('cancelled','returned') GROUP BY 1 ORDER BY s DESC, 1 LIMIT %s")
W = "tenant_id = %s AND created_at >= %s AND created_at < %s"


def flush(conns, api):
    """Emit one marker into every store database's WAL; wait until every stream has applied it."""
    token = uuid.uuid4().hex
    for conn in conns.values():
        with conn.cursor() as c:
            c.execute("SELECT pg_logical_emit_message(true, 'zeta', %s)", (token,))
        conn.commit()
    t0 = time.time()
    while time.time() - t0 < 120:
        if not api.get(f"/markers/{token}", headers={"X-API-Key": admin_key()}).json()["missing"]:
            return time.time() - t0
        time.sleep(0.01)
    raise TimeoutError("ZETA did not apply the marker on every store database within 120s")


def rand_range(rng, lo, hi):
    """Mix of range shapes: second precision, µs precision, hour/day aligned, tiny, open-ish."""
    kind = rng.choice(["sec", "usec", "hour", "day", "tiny", "wide", "recent"])
    span = (hi - lo).total_seconds()
    a = lo + timedelta(seconds=rng.uniform(-86400, span))
    if kind == "usec":
        b = a + timedelta(microseconds=rng.randint(1, int(30 * 86400 * 1e6)))
    elif kind == "hour":
        a = a.replace(minute=0, second=0, microsecond=0)
        b = a + timedelta(hours=rng.randint(1, 500))
    elif kind == "day":
        a = a.replace(hour=0, minute=0, second=0, microsecond=0)
        b = a + timedelta(days=rng.randint(1, 60))
    elif kind == "tiny":
        a = a.replace(microsecond=0)
        b = a + timedelta(seconds=rng.randint(0, 5400))
    elif kind == "wide":
        a, b = lo - timedelta(days=5), hi + timedelta(days=5)
    elif kind == "recent":
        a = hi - timedelta(seconds=rng.uniform(0, 3 * 86400))
        b = hi + timedelta(hours=1)
    else:
        a = a.replace(microsecond=0)
        b = a + timedelta(seconds=rng.randint(1, 45 * 86400))
    return kind, a, b


def timed(fn):
    t = time.perf_counter()
    out = fn()
    return out, (time.perf_counter() - t) * 1000


def check_store(rng, curs, lo, hi, api):
    tid = rng.choice(sorted(curs))
    cur = curs[tid]
    hdr = {"X-API-Key": tenant_key(tid)}  # FR-5: each query uses a key bound to its store
    rkind, a, b = rand_range(rng, lo, hi)
    q = rng.choice(["count", "count_status", "by_status", "trend_hour", "trend_day", "top"])
    p = {"from": a.isoformat(), "to": b.isoformat()}
    args = (tid, a, b)
    status = rng.choice(STATUSES)

    def run(sql, sargs):
        cur.execute(sql, sargs)
        return cur.fetchall()

    if q in ("count", "count_status"):
        sql = f"SELECT count(*), coalesce(sum(total_cents),0) FROM orders WHERE {W}"
        if q == "count_status":
            sql += " AND status = %s"
            args += (status,)
            p["status"] = status
        rows, sql_ms = timed(lambda: run(sql, args))
        exp = tuple(int(x) for x in rows[0])
        r, api_ms = timed(lambda: api.get(f"/tenants/{tid}/orders/count", params=p, headers=hdr).json())
        got = (r["count"], r["revenue_cents"])
    elif q == "by_status":
        rows, sql_ms = timed(lambda: run(f"SELECT status, count(*), sum(total_cents) FROM orders WHERE {W} GROUP BY 1", args))
        exp = {s: (int(c), int(r_)) for s, c, r_ in rows}
        r, api_ms = timed(lambda: api.get(f"/tenants/{tid}/orders/by-status", params=p, headers=hdr).json())
        got = {s: (v["count"], v["revenue_cents"]) for s, v in r["by_status"].items() if v["count"]}
    elif q.startswith("trend"):
        g = q.split("_")[1]
        p["granularity"] = g
        rows, sql_ms = timed(lambda: run(f"SELECT date_trunc('{g}', created_at), count(*), sum(total_cents) "
                                         f"FROM orders WHERE {W} GROUP BY 1 ORDER BY 1", args))
        exp = [(d.astimezone(UTC).isoformat(), int(c), int(r_)) for d, c, r_ in rows]
        r, api_ms = timed(lambda: api.get(f"/tenants/{tid}/orders/trend", params=p, headers=hdr).json())
        got = [(x["bucket"], x["count"], x["revenue_cents"]) for x in r["buckets"] if x["count"]]
    else:
        n = rng.choice([1, 5, 10, 10, 10, 50])
        p["n"] = n
        rows, sql_ms = timed(lambda: run(TOP_SQL, args + (n,)))
        exp = [(int(pid), int(s)) for pid, s in rows]
        r, api_ms = timed(lambda: api.get(f"/tenants/{tid}/products/top", params=p, headers=hdr).json())
        got = [(x["product_id"], x["sales_cents"]) for x in r["products"]]
    nonempty = bool(exp[0]) if isinstance(exp, tuple) else bool(exp)
    return dict(q=q, who=f"store {tid}", range=rkind, ok=got == exp, nonempty=nonempty, api_ms=api_ms,
                sql_ms=sql_ms, engine_ms=r.get("took_ms"), detail=None if got == exp else (p, str(exp)[:300], str(got)[:300]))


def check_owner(rng, curs, lo, hi, api):
    """Owner routes vs SQL run in every included store database and combined by hand."""
    all_tids = sorted(curs)
    tids = all_tids if rng.random() < 0.7 else sorted(rng.sample(all_tids, rng.randint(1, min(5, len(all_tids)))))
    hdr = {"X-API-Key": owner_key()}
    rkind, a, b = rand_range(rng, lo, hi)
    q = rng.choice(["owner_count", "owner_by_store", "owner_by_status", "owner_trend_hour", "owner_trend_day", "owner_top",
                    "owner_category"])
    p = {"from": a.isoformat(), "to": b.isoformat()}
    if tids != all_tids:
        p["stores"] = ",".join(map(str, tids))
    status = rng.choice(STATUSES + [None, None])

    def each(sql, extra=()):
        out = {}
        for t in tids:
            curs[t].execute(sql, (t, a, b) + extra)
            out[t] = curs[t].fetchall()
        return out

    if q in ("owner_count", "owner_by_store"):
        sql = f"SELECT count(*), coalesce(sum(total_cents),0) FROM orders WHERE {W}" + (" AND status = %s" if status else "")
        per, sql_ms = timed(lambda: each(sql, (status,) if status else ()))
        if status:
            p["status"] = status
        path = "/owner/orders/count" if q == "owner_count" else "/owner/orders/by-store"
        r, api_ms = timed(lambda: api.get(path, params=p, headers=hdr).json())
        if q == "owner_count":
            exp = (sum(int(v[0][0]) for v in per.values()), sum(int(v[0][1]) for v in per.values()))
            got = (r["count"], r["revenue_cents"])
        else:
            exp = {t: (int(v[0][0]), int(v[0][1])) for t, v in per.items()}
            got = {x["tenant_id"]: (x["count"], x["revenue_cents"]) for x in r["by_store"]}
    elif q == "owner_by_status":
        per, sql_ms = timed(lambda: each(f"SELECT status, count(*), sum(total_cents) FROM orders WHERE {W} GROUP BY 1"))
        acc = defaultdict(lambda: [0, 0])
        for rows in per.values():
            for s, c, r_ in rows:
                acc[s][0] += int(c)
                acc[s][1] += int(r_)
        exp = {s: tuple(v) for s, v in acc.items()}
        r, api_ms = timed(lambda: api.get("/owner/orders/by-status", params=p, headers=hdr).json())
        got = {s: (v["count"], v["revenue_cents"]) for s, v in r["by_status"].items() if v["count"]}
    elif q.startswith("owner_trend"):
        g = q.rsplit("_", 1)[1]
        p["granularity"] = g
        per, sql_ms = timed(lambda: each(f"SELECT date_trunc('{g}', created_at), count(*), sum(total_cents) "
                                         f"FROM orders WHERE {W} GROUP BY 1"))
        acc = defaultdict(lambda: [0, 0])
        for rows in per.values():
            for d, c, r_ in rows:
                k = d.astimezone(UTC).isoformat()
                acc[k][0] += int(c)
                acc[k][1] += int(r_)
        exp = sorted((k, v[0], v[1]) for k, v in acc.items())
        r, api_ms = timed(lambda: api.get("/owner/orders/trend", params=p, headers=hdr).json())
        got = [(x["bucket"], x["count"], x["revenue_cents"]) for x in r["buckets"] if x["count"]]
    elif q == "owner_category":
        per, sql_ms = timed(lambda: each(
            "SELECT p.category, sum(i.qty::bigint * i.price_cents) FROM order_items i "
            "JOIN orders o ON o.order_id = i.order_id JOIN products p ON p.product_id = i.product_id "
            "WHERE o.tenant_id = %s AND o.created_at >= %s AND o.created_at < %s "
            "AND o.status NOT IN ('cancelled','returned') GROUP BY 1"))
        acc = defaultdict(int)
        for rows in per.values():
            for cat, v in rows:
                acc[cat] += int(v)
        exp = {k: v for k, v in acc.items() if v}
        r, api_ms = timed(lambda: api.get("/analytics/explore", params={**p, "metric": "sales", "by": "category"},
                                          headers=hdr).json())
        got = {x["key"]: x["value"] for x in r["rows"]}
    else:
        n = rng.choice([1, 5, 10, 10, 50])
        p["n"] = n
        per, sql_ms = timed(lambda: each(TOP_SQL, (n,)))
        cands = sorted((-int(s), int(pid), t) for t, rows in per.items() for pid, s in rows)[:n]
        exp = [(pid, -neg, t) for neg, pid, t in cands]
        r, api_ms = timed(lambda: api.get("/owner/products/top", params=p, headers=hdr).json())
        got = [(x["product_id"], x["sales_cents"], x["tenant_id"]) for x in r["products"]]
    nonempty = bool(exp[0]) if isinstance(exp, tuple) else any(exp.values()) if isinstance(exp, dict) else bool(exp)
    return dict(q=q, who=f"owner ({len(tids)} stores)", range=rkind, ok=got == exp, nonempty=nonempty,
                api_ms=api_ms, sql_ms=sql_ms, engine_ms=r.get("took_ms"),
                detail=None if got == exp else (p, str(exp)[:300], str(got)[:300]))


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))] if xs else float("nan")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dbs", default="t1m_*", help="store databases: prefix pattern or comma list")
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--n", type=int, default=1000, help="store checks")
    ap.add_argument("--owner-n", type=int, default=None, help="owner checks (default n/5)")
    ap.add_argument("--mutate", type=int, default=0, help="write transactions to apply before checking")
    ap.add_argument("--seed", type=int, default=1)
    a = ap.parse_args()
    rng = random.Random(a.seed)
    api = httpx.Client(base_url=a.api, timeout=60)
    dbs = discover(PG, a.dbs)
    conns = {db: psycopg2.connect(f"{PG} dbname={db}") for db in dbs}

    if a.mutate:
        print(f"applying {a.mutate} write transactions across {len(dbs)} store databases ...")
        subprocess.run([sys.executable, "simulate.py", "--dbs", a.dbs, "--ops", str(a.mutate),
                        "--seed", str(a.seed)], check=True)
    waited = flush(conns, api)
    print(f"ZETA caught up with all {len(dbs)} store databases (markers visible after {waited * 1000:.0f} ms)")

    curs = {}
    for db, conn in conns.items():
        c = conn.cursor()
        c.execute("SET TIME ZONE 'UTC'")
        c.execute("SELECT tenant_id FROM tenants")
        for (tid,) in c.fetchall():
            curs[tid] = c
    # range endpoints come from where the data is (the biggest store; 'wide' ranges cover outliers)
    big = curs[min(curs)]
    big.execute("SELECT percentile_disc(0.0001) WITHIN GROUP (ORDER BY created_at), "
                "percentile_disc(0.9999) WITHIN GROUP (ORDER BY created_at) FROM orders")
    lo, hi = big.fetchone()

    owner_n = a.n // 5 if a.owner_n is None else a.owner_n
    results = [check_store(rng, curs, lo, hi, api) for _ in range(a.n)]
    results += [check_owner(rng, curs, lo, hi, api) for _ in range(owner_n)]
    for conn in conns.values():
        conn.rollback()

    by = defaultdict(list)
    for r in results:
        by[r["q"]].append(r)
    print(f"\n{'query':<18}{'checks':>8}{'non-empty':>10}{'match':>8}{'api p50':>10}{'api p99':>10}"
          f"{'engine p50':>12}{'sql p50':>10}")
    for q, rs in sorted(by.items(), key=lambda kv: (kv[0].startswith("owner"), kv[0])):
        print(f"{q:<18}{len(rs):>8}{sum(r['nonempty'] for r in rs):>10}{sum(r['ok'] for r in rs):>8}"
              f"{pct([r['api_ms'] for r in rs], 50):>9.2f}ms{pct([r['api_ms'] for r in rs], 99):>8.2f}ms"
              f"{pct([r['engine_ms'] for r in rs], 50):>10.3f}ms{pct([r['sql_ms'] for r in rs], 50):>8.1f}ms")
    ok = sum(r["ok"] for r in results)
    print(f"\nTOTAL {ok}/{len(results)} match direct SQL ({a.n} store checks, {owner_n} owner checks "
          f"across {len(dbs)} store databases)")
    for r in [r for r in results if not r["ok"]][:5]:
        print("MISMATCH", r["q"], r["who"], r["range"], *r["detail"], sep="\n  ")
    health = api.get("/health", headers={"X-API-Key": admin_key()}).json()
    if health.get("errors"):
        print("engine errors:", health["errors"], health["last_error"])
    sys.exit(0 if ok == len(results) else 1)


if __name__ == "__main__":
    main()
