#!/usr/bin/env python3
"""ZETA benchmarks with one Postgres database per store: Speed + Growth, Freshness, DB safety.

  python bench.py latency     # biggest store + owner (all stores), 1M (10 DBs) vs 10M (100 DBs), ZETA vs direct SQL
  python bench.py freshness   # commit in a store database -> visible in ZETA, idle and under write load
  python bench.py safety      # app-query latency + Postgres CPU: no dashboards / dashboards on ZETA / on SQL
  python bench.py all

Expects ZETA on :8000 (t1m_*, 1M) and :8001 (t10m_*, 10M). Direct SQL for owner questions means what an
owner would do without ZETA: run the query in every store database and add the answers up.
"""
import argparse
import multiprocessing as mp
import random
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import httpx
import psycopg2

from zeta.cdc import discover
from zeta.guard import admin_key

PG = "host=localhost port=5433 user=zeta"
SETUPS = [("1M", "t1m", "http://localhost:8000"), ("10M", "t10m", "http://localhost:8001")]
END = datetime(2026, 10, 31, 9, 0, tzinfo=timezone.utc)  # generator --end
H = {"X-API-Key": admin_key()}  # admin key: not rate limited
W = "tenant_id = %s AND created_at >= %s AND created_at < %s"
TOP = ("SELECT i.product_id, sum(i.qty::bigint * i.price_cents) s FROM order_items i JOIN orders o ON o.order_id = i.order_id "
       "WHERE o.tenant_id = %s AND o.created_at >= %s AND o.created_at < %s AND o.status NOT IN ('cancelled','returned') "
       "GROUP BY 1 ORDER BY s DESC, 1 LIMIT 10")


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))] if xs else float("nan")


def ts(d):
    return d.isoformat()


def store_queries(tid):
    """(name, api path, params, sql, sql args) for one store, with non-aligned edges."""
    f7, f30, f90 = END - timedelta(days=7, minutes=-17, seconds=23), END - timedelta(days=30, minutes=41), END - timedelta(days=90)
    f3 = END - timedelta(days=3, seconds=7)
    return [
        ("count 7d", f"/tenants/{tid}/orders/count", {"from": ts(f7), "to": ts(END)},
         f"SELECT count(*), sum(total_cents) FROM orders WHERE {W}", (tid, f7, END)),
        ("count 90d", f"/tenants/{tid}/orders/count", {"from": ts(f90), "to": ts(END)},
         f"SELECT count(*), sum(total_cents) FROM orders WHERE {W}", (tid, f90, END)),
        ("by-status 30d", f"/tenants/{tid}/orders/by-status", {"from": ts(f30), "to": ts(END)},
         f"SELECT status, count(*), sum(total_cents) FROM orders WHERE {W} GROUP BY 1", (tid, f30, END)),
        ("trend hour 3d", f"/tenants/{tid}/orders/trend", {"from": ts(f3), "to": ts(END), "granularity": "hour"},
         f"SELECT date_trunc('hour', created_at), count(*), sum(total_cents) FROM orders WHERE {W} GROUP BY 1", (tid, f3, END)),
        ("trend day 90d", f"/tenants/{tid}/orders/trend", {"from": ts(f90), "to": ts(END), "granularity": "day"},
         f"SELECT date_trunc('day', created_at), count(*), sum(total_cents) FROM orders WHERE {W} GROUP BY 1", (tid, f90, END)),
        ("top10 30d", f"/tenants/{tid}/products/top", {"from": ts(f30), "to": ts(END), "n": 10}, TOP, (tid, f30, END)),
        ("top10 90d", f"/tenants/{tid}/products/top", {"from": ts(f90), "to": ts(END), "n": 10}, TOP, (tid, f90, END)),
    ]


def owner_queries():
    """Owner questions across every store; SQL runs per store database (tid filled in per database)."""
    f7, f30, f90 = END - timedelta(days=7, minutes=-17, seconds=23), END - timedelta(days=30, minutes=41), END - timedelta(days=90)
    return [
        ("OWNER count 7d", "/owner/orders/count", {"from": ts(f7), "to": ts(END)},
         f"SELECT count(*), sum(total_cents) FROM orders WHERE {W}", (f7, END)),
        ("OWNER by-store 30d", "/owner/orders/by-store", {"from": ts(f30), "to": ts(END)},
         f"SELECT count(*), sum(total_cents) FROM orders WHERE {W}", (f30, END)),
        ("OWNER trend day 90d", "/owner/orders/trend", {"from": ts(f90), "to": ts(END), "granularity": "day"},
         f"SELECT date_trunc('day', created_at), count(*), sum(total_cents) FROM orders WHERE {W} GROUP BY 1", (f90, END)),
        ("OWNER top10 30d", "/owner/products/top", {"from": ts(f30), "to": ts(END), "n": 10}, TOP, (f30, END)),
    ]


def store_cursors(prefix):
    curs = {}
    for db in discover(PG, prefix + "_*"):
        c = psycopg2.connect(f"{PG} dbname={db}").cursor()
        c.execute("SET TIME ZONE 'UTC'")
        c.execute("SELECT tenant_id FROM tenants")
        for (tid,) in c.fetchall():
            curs[tid] = c
    return curs


# ---------------- latency / growth ----------------
def bench_latency(reps, sql_reps):
    res = {}
    names = [q[0] for q in store_queries(1)] + [q[0] for q in owner_queries()]
    for label, prefix, api_url in SETUPS:
        api = httpx.Client(base_url=api_url, headers=H, timeout=120)
        curs = store_cursors(prefix)
        curs[1].execute("SELECT count(*) FROM orders")
        print(f"\n[{label}] {len(set(map(id, curs.values())))} store databases · biggest store = {curs[1].fetchone()[0]:,} orders")
        jobs = [(n, p, q, lambda s=sql, a=args: (curs[1].execute(s, a), curs[1].fetchall()))
                for n, p, q, sql, args in store_queries(1)]
        jobs += [(n, p, q, lambda s=sql, a=args: [(curs[t].execute(s, (t,) + a), curs[t].fetchall()) for t in curs])
                 for n, p, q, sql, args in owner_queries()]
        for name, path, params, run_sql in jobs:
            for _ in range(10):
                api.get(path, params=params)
            api_ms, eng_ms = [], []
            for _ in range(reps):
                t0 = time.perf_counter()
                r = api.get(path, params=params).json()
                api_ms.append((time.perf_counter() - t0) * 1000)
                eng_ms.append(r["took_ms"])
            sql_ms = []
            for _ in range(sql_reps if not name.startswith("OWNER") else max(2, sql_reps // 3)):
                t0 = time.perf_counter()
                run_sql()
                sql_ms.append((time.perf_counter() - t0) * 1000)
            res[(label, name)] = (pct(api_ms, 50), pct(api_ms, 99), pct(eng_ms, 50), pct(sql_ms, 50))

    print(f"\n{'query':<20}{'ZETA p50 1M':>12}{'10M':>9}{'p99 10M':>9}{'engine 1M':>11}{'10M':>9}"
          f"{'growth':>8}{'SQL p50 1M':>12}{'10M':>10}")
    for name in names:
        a, b = res[("1M", name)], res[("10M", name)]
        print(f"{name:<20}{a[0]:>10.2f}ms{b[0]:>7.2f}ms{b[1]:>7.2f}ms{a[2]:>9.3f}ms{b[2]:>7.3f}ms"
              f"{b[0] / a[0]:>7.2f}x{a[3]:>10.1f}ms{b[3]:>8.1f}ms")
    print("(store rows: biggest store, 374k orders at 1M vs 2.3M at 10M · OWNER rows: 10 vs 100 store databases)")


# ---------------- freshness ----------------
def bench_freshness(samples, prefix="t10m", api_url="http://localhost:8001"):
    api = httpx.Client(base_url=api_url, timeout=30)
    db = f"{prefix}_001"
    conn = psycopg2.connect(f"{PG} dbname={db}")
    cur = conn.cursor()
    cur.execute("SELECT least(min(order_id), 0) FROM orders")
    oid = cur.fetchone()[0]  # probe orders get negative ids: never collide with the app's max(order_id)+1
    conn.commit()

    def run(tag):
        nonlocal oid
        out = []
        for _ in range(samples):
            oid -= 1
            token = f"fresh-{uuid.uuid4().hex}"
            cur.execute("INSERT INTO orders SELECT %s, 1, customer_id, 'placed', 999, now(), now() "
                        "FROM customers LIMIT 1", (oid,))
            cur.execute("SELECT pg_logical_emit_message(true, 'zeta', %s)", (token,))
            t0 = time.perf_counter()
            conn.commit()
            while db in api.get(f"/markers/{token}").json()["missing"]:
                time.sleep(0.001)
            out.append((time.perf_counter() - t0) * 1000)
            time.sleep(0.02)
        print(f"  {tag:<34} p50 {pct(out, 50):6.1f} ms   p99 {pct(out, 99):6.1f} ms   max {max(out):6.1f} ms")
        return out

    print(f"\nFreshness: order committed in store database {db} -> visible in ZETA ({samples} samples)")
    run("idle")
    sim = subprocess.Popen([sys.executable, "simulate.py", "--dbs", f"{prefix}_*", "--rate", "300",
                            "--seconds", "600", "--seed", "77"], stdout=subprocess.DEVNULL)
    time.sleep(2)
    try:
        run("under 300 write tx/s, all stores")
    finally:
        sim.terminate()
    cur.execute("DELETE FROM orders WHERE order_id < 0")
    conn.commit()


# ---------------- DB safety ----------------
def _pg_cores():
    """Instantaneous CPU of all postgres processes (incl. short-lived parallel workers), in cores."""
    out = subprocess.run(["ps", "-axo", "pcpu=,comm="], capture_output=True, text=True).stdout
    return sum(float(l.split(None, 1)[0]) for l in out.splitlines() if "postgres" in l) / 100


def _sample_cpu(deadline, q):
    xs = []
    while time.time() < deadline - 0.5:
        xs.append(_pg_cores())
        time.sleep(0.5)
    q.put(("cpu", sum(xs) / max(len(xs), 1)))


def _app_worker(prefix, deadline, q):
    """The store app: point lookups by primary key and small updates, spread over the store databases."""
    rng = random.Random()
    dbs = discover(PG, prefix + "_*")
    conns = {}
    ranges = {}
    lat = []
    while time.time() < deadline:
        db = dbs[min(int(rng.paretovariate(1.2)) - 1, len(dbs) - 1)]
        if db not in conns:
            conns[db] = psycopg2.connect(f"{PG} dbname={db}")
            c = conns[db].cursor()
            c.execute("SELECT min(order_id), max(order_id) FROM orders WHERE order_id > 0")
            ranges[db] = c.fetchone()
            conns[db].commit()
        conn, (lo, hi) = conns[db], ranges[db]
        cur = conn.cursor()
        oid = rng.randint(lo, hi)
        t0 = time.perf_counter()
        if rng.random() < 0.8:
            cur.execute("SELECT o.*, (SELECT count(*) FROM order_items i WHERE i.order_id = o.order_id) "
                        "FROM orders o WHERE order_id = %s", (oid,))
            cur.fetchall()
        else:
            cur.execute("UPDATE orders SET updated_at = now() WHERE order_id = %s", (oid,))
        conn.commit()
        lat.append((time.perf_counter() - t0) * 1000)
        time.sleep(0.002)
    q.put(lat)


def _dash_worker(mode, prefix, api_url, deadline, rate, q):
    """A dashboard user: `rate` widget refreshes per second (or as fast as the backend allows).
    Half the widgets are a store's own numbers, half are the owner's all-store numbers."""
    rng = random.Random()
    n = 0
    t_start = time.time()
    if mode == "zeta":
        api = httpx.Client(base_url=api_url, headers=H, timeout=120)
    else:
        curs = store_cursors(prefix)
    while time.time() < deadline:
        if rng.random() < 0.5:
            tid = min(int(rng.paretovariate(1.0)), max(curs) if mode != "zeta" else 100)
            name, path, params, sql, args = rng.choice(store_queries(tid))
            if mode == "zeta":
                api.get(path, params=params)
            else:
                curs[tid].execute(sql, args)
                curs[tid].fetchall()
        else:
            name, path, params, sql, args = rng.choice(owner_queries())
            if mode == "zeta":
                api.get(path, params=params)
            else:
                for t, c in curs.items():
                    c.execute(sql, (t,) + args)
                    c.fetchall()
        n += 1
        time.sleep(max(0.0, t_start + n / rate - time.time()))
    q.put(n)


def bench_safety(seconds, app_procs=4, dash_procs=8, rate=5.0, prefix="t10m", api_url="http://localhost:8001"):
    print(f"\nDB safety ({prefix}_*: one database per store, {app_procs} app workers, {dash_procs} dashboard users x "
          f"{rate:g} refresh/s offered, {seconds}s each; ZETA's 60 s audit keeps running throughout)")
    print(f"  {'scenario':<22}{'app p50':>10}{'app p99':>10}{'Δp50':>7}{'Δp99':>7}{'PG CPU':>12}{'dash q/s':>10}")
    base = None
    for scen in ("no dashboards", "dashboards on ZETA", "dashboards on SQL"):
        q = mp.Queue()
        deadline = time.time() + seconds + 1
        procs = [mp.Process(target=_app_worker, args=(prefix, deadline, q)) for _ in range(app_procs)]
        mode = {"dashboards on ZETA": "zeta", "dashboards on SQL": "sql"}.get(scen)
        if mode:
            procs += [mp.Process(target=_dash_worker, args=(mode, prefix, api_url, deadline, rate, q))
                      for _ in range(dash_procs)]
        procs.append(mp.Process(target=_sample_cpu, args=(deadline, q)))
        for p in procs:
            p.start()
        results = [q.get() for _ in procs]
        for p in procs:
            p.join()
        lat = [x for r in results if isinstance(r, list) for x in r]
        dq = sum(r for r in results if isinstance(r, int)) / seconds
        cpu = next(r[1] for r in results if isinstance(r, tuple))
        p50, p99 = pct(lat, 50), pct(lat, 99)
        base = base or (p50, p99)
        print(f"  {scen:<22}{p50:>8.2f}ms{p99:>8.2f}ms{(p50 / base[0] - 1) * 100:>6.0f}%{(p99 / base[1] - 1) * 100:>6.0f}%"
              f"{cpu:>7.2f} cores{dq:>10.1f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["latency", "freshness", "safety", "all"])
    ap.add_argument("--reps", type=int, default=200)
    ap.add_argument("--sql-reps", type=int, default=6)
    ap.add_argument("--samples", type=int, default=100)
    ap.add_argument("--seconds", type=int, default=70, help="per safety scenario (>60 s includes an audit)")
    ap.add_argument("--rate", type=float, default=10.0, help="refreshes/s per dashboard user")
    ap.add_argument("--dash-users", type=int, default=4,
                    help="dashboard users (each needs a connection to every store database in SQL mode)")
    a = ap.parse_args()
    if a.what in ("latency", "all"):
        bench_latency(a.reps, a.sql_reps)
    if a.what in ("freshness", "all"):
        bench_freshness(a.samples)
    if a.what in ("safety", "all"):
        bench_safety(a.seconds, dash_procs=a.dash_users, rate=a.rate)


if __name__ == "__main__":
    main()
