#!/usr/bin/env python3
"""Live judge demo: one tenant's customers place orders, then cancel some.

For every step it shows
  * the write going into Postgres (the store app's normal INSERT / UPDATE),
  * how long until ZETA's API reflects it (commit -> visible, measured end to end over HTTP),
  * the dashboard numbers (count, by-status, top products) before and after,
  * the same numbers from a direct SQL query, to prove they match,
  * another tenant's numbers, to prove nothing leaked across tenants.

  python demo_live.py                         # tenant 5 on the 10M server
  python demo_live.py --tenant 1 --orders 3 --cancel 2
  python demo_live.py --api http://localhost:8000 --db zeta
"""
import argparse
import random
import time
from datetime import datetime, timedelta, timezone

import httpx
import psycopg2

from zeta.guard import tenant_key

UTC = timezone.utc


def rupees(cents):
    return f"₹{cents / 100:,.2f}"


class Demo:
    def __init__(self, a):
        self.a = a
        self.tid = a.tenant
        self.other = a.other
        self.db = psycopg2.connect(f"host=localhost port=5433 user=zeta dbname={a.db}")
        self.api = httpx.Client(base_url=a.api, timeout=10)
        self.hdr = {"X-API-Key": tenant_key(self.tid)}
        now = datetime.now(UTC)
        day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        self.win = {"from": day.isoformat(), "to": (day + timedelta(days=1)).isoformat()}  # "today" (UTC)
        self.latencies = []
        self.created = []

    # ---------- reads ----------
    def zeta(self, tid=None):
        tid = tid or self.tid
        h = {"X-API-Key": tenant_key(tid)}
        c = self.api.get(f"/tenants/{tid}/orders/count", params=self.win, headers=h).json()
        s = self.api.get(f"/tenants/{tid}/orders/by-status", params=self.win, headers=h).json()
        t = self.api.get(f"/tenants/{tid}/products/top", params={**self.win, "n": 3}, headers=h).json()
        return {"count": c["count"], "revenue": c["revenue_cents"],
                "status": {k: v["count"] for k, v in s["by_status"].items()},
                "top": [(p["product_id"], p["sales_cents"]) for p in t["products"]],
                "lag": c["freshness"]["cdc_lag_ms"]}

    def sql(self):
        with self.db.cursor() as c:
            c.execute("SELECT count(*), coalesce(sum(total_cents),0) FROM orders "
                      "WHERE tenant_id=%s AND created_at >= %s AND created_at < %s",
                      (self.tid, self.win["from"], self.win["to"]))
            cnt, rev = c.fetchone()
            c.execute("SELECT i.product_id, sum(i.qty::bigint*i.price_cents) s FROM order_items i "
                      "JOIN orders o ON o.order_id=i.order_id WHERE o.tenant_id=%s AND o.created_at >= %s "
                      "AND o.created_at < %s AND o.status NOT IN ('cancelled','returned') "
                      "GROUP BY 1 ORDER BY s DESC, 1 LIMIT 3", (self.tid, self.win["from"], self.win["to"]))
            top = [(int(p), int(s)) for p, s in c.fetchall()]
        self.db.rollback()
        return {"count": int(cnt), "revenue": int(rev), "top": top}

    def wait_until(self, pred, t_commit):
        """Poll ZETA's API until pred(state) holds; return (state, ms since commit)."""
        while True:
            z = self.zeta()
            if pred(z):
                ms = (time.perf_counter() - t_commit) * 1000
                self.latencies.append(ms)
                return z, ms
            if time.perf_counter() - t_commit > 30:
                raise TimeoutError("ZETA did not reflect the change within 30 s")

    def show(self, z, label="ZETA"):
        st = ", ".join(f"{k} {v}" for k, v in z["status"].items() if v)
        print(f"     {label:<5} today: {z['count']:,} orders, {rupees(z['revenue'])} | {st}")
        print(f"            top products: " + ", ".join(f"#{p} {rupees(s)}" for p, s in z["top"]))

    def verify(self, z):
        q = self.sql()
        ok = q["count"] == z["count"] and q["revenue"] == z["revenue"] and q["top"] == z["top"]
        print(f"     SQL   today: {q['count']:,} orders, {rupees(q['revenue'])}  ->  "
              f"{'MATCHES ZETA' if ok else 'MISMATCH!'} (count, revenue, top-3)")
        return ok

    # ---------- writes (what the store app does) ----------
    def place_order(self, product_ids):
        with self.db.cursor() as c:
            c.execute("SELECT max(order_id) + 1 FROM orders")
            oid = c.fetchone()[0]
            c.execute("SELECT customer_id, name, city FROM customers WHERE tenant_id=%s "
                      "ORDER BY random() LIMIT 1", (self.tid,))
            cust, cname, city = c.fetchone()
            lines = []
            for pid in product_ids:
                c.execute("SELECT price_cents FROM products WHERE product_id=%s", (pid,))
                lines.append((pid, random.randint(1, 3), c.fetchone()[0]))
            total = sum(q * p for _, q, p in lines)
            c.execute("INSERT INTO orders VALUES (%s,%s,%s,'placed',%s,now(),now())", (oid, self.tid, cust, total))
            for n, (pid, q, p) in enumerate(lines, 1):
                c.execute("INSERT INTO order_items VALUES (%s,%s,%s,%s,%s,%s)", (oid, n, self.tid, pid, q, p))
        t0 = time.perf_counter()
        self.db.commit()
        self.created.append(oid)
        return oid, total, lines, f"{cname} ({city})", t0

    def set_status(self, oid, status):
        with self.db.cursor() as c:
            c.execute("UPDATE orders SET status=%s, updated_at=now() WHERE order_id=%s", (status, oid))
        t0 = time.perf_counter()
        self.db.commit()
        return t0

    def delete(self, oid):
        with self.db.cursor() as c:
            c.execute("DELETE FROM order_items WHERE order_id=%s", (oid,))
            c.execute("DELETE FROM orders WHERE order_id=%s", (oid,))
        t0 = time.perf_counter()
        self.db.commit()
        return t0

    # ---------- the story ----------
    def run(self):
        a = self.a
        print(f"\nZETA live demo · tenant {self.tid} · API {a.api} · Postgres db '{a.db}' · window = today (UTC)\n")
        z0 = self.zeta()
        other0 = self.zeta(self.other)
        print("0) Before")
        self.show(z0)
        ok = self.verify(z0)
        hot = [p for p, _ in z0["top"][:1]] or []
        with self.db.cursor() as c:
            c.execute("SELECT product_id FROM products WHERE tenant_id=%s ORDER BY random() LIMIT 2", (self.tid,))
            rnd = [r[0] for r in c.fetchall()]
        self.db.rollback()

        z = z0
        placed = []
        for i in range(a.orders):
            prods = (hot + rnd[:1]) if i == 0 else rnd[i % 2:i % 2 + 1]
            oid, total, lines, who, t0 = self.place_order(prods)
            items = ", ".join(f"{q} x #{p}" for p, q, _ in lines)
            print(f"\n{i + 1}) Customer {who} PLACES order #{oid}: {items} = {rupees(total)}")
            before = z["count"]
            z, ms = self.wait_until(lambda s: s["count"] == before + 1, t0)
            print(f"     visible in ZETA {ms:.1f} ms after COMMIT (CDC lag reported {z['lag']} ms)")
            self.show(z)
            ok &= self.verify(z)
            placed.append(oid)

        for j, oid in enumerate(placed[:a.cancel]):
            print(f"\n{a.orders + j + 1}) Customer CANCELS order #{oid}")
            before = z["status"].get("cancelled", 0)
            t0 = self.set_status(oid, "cancelled")
            z, ms = self.wait_until(lambda s: s["status"].get("cancelled", 0) == before + 1, t0)
            print(f"     visible in ZETA {ms:.1f} ms after COMMIT: placed -1, cancelled +1, "
                  f"its item sales retracted from top products (order count unchanged: a cancelled order is still an order)")
            self.show(z)
            ok &= self.verify(z)

        step = a.orders + a.cancel + 1
        print(f"\n{step}) Burst: a customer places an order and cancels it immediately (two transactions, back to back)")
        oid, total, _, who, _ = self.place_order(rnd[:1])
        before_c = z["status"].get("cancelled", 0)
        before_n = z["count"]
        t0 = self.set_status(oid, "cancelled")
        z, ms = self.wait_until(lambda s: s["count"] == before_n + 1 and s["status"].get("cancelled", 0) == before_c + 1, t0)
        print(f"     order #{oid} ({rupees(total)}) by {who}: both changes visible {ms:.1f} ms after the cancel COMMIT")
        self.show(z)
        ok &= self.verify(z)

        other1 = self.zeta(self.other)
        same = (other0["count"], other0["revenue"], other0["top"]) == (other1["count"], other1["revenue"], other1["top"])
        print(f"\n{step + 1}) Tenant isolation: tenant {self.other}'s numbers before/after: "
              f"{other0['count']:,} -> {other1['count']:,} orders ({'unchanged' if same else 'CHANGED?'})")
        r = self.api.get(f"/tenants/{self.other}/orders/count", headers=self.hdr)
        print(f"     tenant {self.tid}'s API key on tenant {self.other}'s URL -> HTTP {r.status_code} ({r.json()['detail']})")

        if not a.keep:
            print(f"\n{step + 2}) Cleanup: DELETE the {len(self.created)} demo orders")
            target = z0["count"]
            t0 = None
            for oid in self.created:
                t0 = self.delete(oid)
            z, ms = self.wait_until(lambda s: s["count"] == target, t0)
            print(f"     back to the starting numbers {ms:.1f} ms after the last COMMIT")
            self.show(z)
            ok &= self.verify(z) and z["top"] == z0["top"] and z["revenue"] == z0["revenue"]

        lat = sorted(self.latencies)
        print(f"\nCommit -> visible in ZETA's API, {len(lat)} changes: min {lat[0]:.1f} ms, "
              f"median {lat[len(lat) // 2]:.1f} ms, max {lat[-1]:.1f} ms")
        print(f"Every step matched direct SQL: {'YES' if ok else 'NO'}\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tenant", type=int, default=5)
    ap.add_argument("--other", type=int, default=2, help="another tenant, to show isolation")
    ap.add_argument("--orders", type=int, default=3)
    ap.add_argument("--cancel", type=int, default=2)
    ap.add_argument("--api", default="http://localhost:8001")
    ap.add_argument("--db", default="zeta10m")
    ap.add_argument("--keep", action="store_true", help="keep the demo orders (default: delete them at the end)")
    Demo(ap.parse_args()).run()


if __name__ == "__main__":
    main()
