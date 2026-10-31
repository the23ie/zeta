#!/usr/bin/env python3
"""Correctness harness: random (tenant, range, query) checks of the ZETA API against direct SQL.

  python harness.py --n 1000                    # read-only checks
  python harness.py --n 1000 --mutate 2000      # first apply 2000 write transactions, then check

Before checking, a transactional logical message is written and we wait until ZETA has
applied it, so both sides answer from the same committed state.
"""
import argparse
import random
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import httpx
import psycopg2

from zeta.guard import tenant_key

UTC = timezone.utc


def flush(conn, api):
    """Emit a marker into the WAL and wait until ZETA has applied it. Returns seconds waited."""
    token = uuid.uuid4().hex
    with conn.cursor() as c:
        c.execute("SELECT pg_logical_emit_message(true, 'zeta', %s)", (token,))
    conn.commit()
    t0 = time.time()
    while time.time() - t0 < 60:
        if api.get("/health").json().get("last_marker") == token:
            return time.time() - t0
        time.sleep(0.01)
    raise TimeoutError("ZETA did not apply the marker within 60s")


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


def check(rng, tenants, lo, hi, api, cur):
    tid = rng.choice(tenants)
    hdr = {"X-API-Key": tenant_key(tid)}  # FR-5: each query uses a key bound to its tenant
    rkind, a, b = rand_range(rng, lo, hi)
    q = rng.choice(["count", "count_status", "by_status", "trend_hour", "trend_day", "top"])
    p = {"from": a.isoformat(), "to": b.isoformat()}
    w = "tenant_id = %s AND created_at >= %s AND created_at < %s"
    args = (tid, a, b)
    status = rng.choice(["placed", "shipped", "delivered", "cancelled", "returned"])

    if q in ("count", "count_status"):
        sql = f"SELECT count(*), coalesce(sum(total_cents),0) FROM orders WHERE {w}"
        if q == "count_status":
            sql += " AND status = %s"
            args += (status,)
            p["status"] = status
        path = f"/tenants/{tid}/orders/count"
        ts = time.perf_counter(); cur.execute(sql, args); exp = tuple(int(x) for x in cur.fetchone())
        sql_ms = (time.perf_counter() - ts) * 1000
        ta = time.perf_counter(); r = api.get(path, params=p, headers=hdr).json(); api_ms = (time.perf_counter() - ta) * 1000
        got = (r["count"], r["revenue_cents"])
    elif q == "by_status":
        sql = f"SELECT status, count(*), sum(total_cents) FROM orders WHERE {w} GROUP BY 1"
        ts = time.perf_counter(); cur.execute(sql, args)
        exp = {s: (int(c), int(r_)) for s, c, r_ in cur.fetchall()}
        sql_ms = (time.perf_counter() - ts) * 1000
        ta = time.perf_counter(); r = api.get(f"/tenants/{tid}/orders/by-status", params=p, headers=hdr).json()
        api_ms = (time.perf_counter() - ta) * 1000
        got = {s: (v["count"], v["revenue_cents"]) for s, v in r["by_status"].items() if v["count"]}
    elif q.startswith("trend"):
        g = q.split("_")[1]
        p["granularity"] = g
        sql = (f"SELECT date_trunc('{g}', created_at), count(*), sum(total_cents) FROM orders "
               f"WHERE {w} GROUP BY 1 ORDER BY 1")
        ts = time.perf_counter(); cur.execute(sql, args)
        exp = [(d.astimezone(UTC).isoformat(), int(c), int(r_)) for d, c, r_ in cur.fetchall()]
        sql_ms = (time.perf_counter() - ts) * 1000
        ta = time.perf_counter(); r = api.get(f"/tenants/{tid}/orders/trend", params=p, headers=hdr).json()
        api_ms = (time.perf_counter() - ta) * 1000
        got = [(x["bucket"], x["count"], x["revenue_cents"]) for x in r["buckets"] if x["count"]]
    else:
        n = rng.choice([1, 5, 10, 10, 10, 50])
        p["n"] = n
        sql = ("SELECT i.product_id, sum(i.qty::bigint * i.price_cents) s FROM order_items i "
               "JOIN orders o ON o.order_id = i.order_id "
               "WHERE o.tenant_id = %s AND o.created_at >= %s AND o.created_at < %s "
               "AND o.status NOT IN ('cancelled','returned') "
               "GROUP BY 1 ORDER BY s DESC, 1 LIMIT %s")
        ts = time.perf_counter(); cur.execute(sql, args + (n,))
        exp = [(int(pid), int(s)) for pid, s in cur.fetchall()]
        sql_ms = (time.perf_counter() - ts) * 1000
        ta = time.perf_counter(); r = api.get(f"/tenants/{tid}/products/top", params=p, headers=hdr).json()
        api_ms = (time.perf_counter() - ta) * 1000
        got = [(x["product_id"], x["sales_cents"]) for x in r["products"]]
    nonempty = bool(exp[0]) if isinstance(exp, tuple) else bool(exp)
    return dict(q=q, tenant=tid, range=rkind, ok=got == exp, nonempty=nonempty, api_ms=api_ms, sql_ms=sql_ms,
                engine_ms=r.get("took_ms"), detail=None if got == exp else (p, str(exp)[:300], str(got)[:300]))


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))] if xs else float("nan")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="host=localhost port=5433 user=zeta dbname=zeta")
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--n", type=int, default=1000)
    ap.add_argument("--mutate", type=int, default=0, help="write transactions to apply before checking")
    ap.add_argument("--seed", type=int, default=1)
    a = ap.parse_args()
    rng = random.Random(a.seed)
    api = httpx.Client(base_url=a.api, timeout=30)
    conn = psycopg2.connect(a.dsn)

    if a.mutate:
        print(f"applying {a.mutate} write transactions ...")
        subprocess.run([sys.executable, "simulate.py", "--dsn", a.dsn, "--ops", str(a.mutate),
                        "--seed", str(a.seed)], check=True)
    waited = flush(conn, api)
    print(f"ZETA caught up with the database (marker visible after {waited * 1000:.0f} ms)")

    cur = conn.cursor()
    cur.execute("SET TIME ZONE 'UTC'")
    # range endpoints come from where the data is (outlier orders are still covered by 'wide' ranges)
    cur.execute("SELECT (SELECT array_agg(tenant_id ORDER BY tenant_id) FROM tenants), "
                "percentile_disc(0.0001) WITHIN GROUP (ORDER BY created_at), "
                "percentile_disc(0.9999) WITHIN GROUP (ORDER BY created_at) FROM orders")
    tenants, lo, hi = cur.fetchone()

    results = [check(rng, tenants, lo, hi, api, cur) for _ in range(a.n)]
    conn.rollback()

    by = defaultdict(list)
    for r in results:
        by[r["q"]].append(r)
    print(f"\n{'query':<14}{'checks':>8}{'non-empty':>10}{'match':>8}{'api p50':>10}{'api p99':>10}{'engine p50':>12}{'sql p50':>10}")
    for q, rs in sorted(by.items()):
        print(f"{q:<14}{len(rs):>8}{sum(r['nonempty'] for r in rs):>10}{sum(r['ok'] for r in rs):>8}"
              f"{pct([r['api_ms'] for r in rs], 50):>9.2f}ms{pct([r['api_ms'] for r in rs], 99):>8.2f}ms"
              f"{pct([r['engine_ms'] for r in rs], 50):>10.3f}ms{pct([r['sql_ms'] for r in rs], 50):>8.1f}ms")
    ok = sum(r["ok"] for r in results)
    print(f"\nTOTAL {ok}/{len(results)} match direct SQL")
    for r in [r for r in results if not r["ok"]][:5]:
        print("MISMATCH", r["q"], "tenant", r["tenant"], r["range"], *r["detail"], sep="\n  ")
    health = api.get("/health").json()
    if health.get("errors"):
        print("engine errors:", health["errors"], health["last_error"])
    sys.exit(0 if ok == len(results) else 1)


if __name__ == "__main__":
    main()
