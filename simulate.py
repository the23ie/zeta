#!/usr/bin/env python3
"""Store-app write traffic: new orders, status changes, cancellations of old orders,
returns, deletes, items added to existing orders, backdated created_at.

  python simulate.py --ops 500                 # burst of 500 transactions
  python simulate.py --rate 20 --seconds 600   # steady 20 tx/s for 10 minutes
"""
import argparse
import random
import time
from datetime import datetime, timedelta, timezone

import psycopg2

NEXT = {"placed": ["shipped", "cancelled"], "shipped": ["delivered", "cancelled"],
        "delivered": ["returned"], "cancelled": ["placed"], "returned": ["delivered"]}


class Store:
    def __init__(self, dsn, seed):
        self.rng = random.Random(seed)
        self.conn = psycopg2.connect(dsn)
        c = self.conn.cursor()
        c.execute("SELECT tenant_id FROM tenants ORDER BY 1")
        self.tenants = [r[0] for r in c.fetchall()]
        c.execute("SELECT tenant_id, product_id, price_cents FROM products")
        self.products = {}
        for t, p, pr in c.fetchall():
            self.products.setdefault(t, []).append((p, pr))
        c.execute("SELECT tenant_id, min(customer_id), max(customer_id) FROM customers GROUP BY 1")
        self.customers = {t: (a, b) for t, a, b in c.fetchall()}
        c.execute("SELECT max(order_id), min(created_at), max(created_at) FROM orders")
        self.max_oid, self.t_min, self.t_max = c.fetchone()
        self.conn.commit()
        self.counts = {}

    def _tenant(self):
        # skewed like the data: low ids are big tenants
        return self.tenants[min(int(self.rng.paretovariate(1.2)) - 1, len(self.tenants) - 1)]

    def _random_order(self, c, recent=False):
        lo = max(1, self.max_oid - 5000) if recent else 1
        oid = self.rng.randint(lo, self.max_oid)
        c.execute("SELECT order_id, tenant_id, status FROM orders WHERE order_id = %s FOR UPDATE", (oid,))
        return c.fetchone()

    def insert_order(self, c, created=None):
        t = self._tenant()
        a, b = self.customers[t]
        self.max_oid += 1
        oid = self.max_oid
        lines = [(self.rng.choice(self.products[t]), self.rng.randint(1, 3)) for _ in range(self.rng.randint(1, 3))]
        total = sum(pr * q for (_, pr), q in lines)
        c.execute("INSERT INTO orders VALUES (%s,%s,%s,'placed',%s,coalesce(%s, now()),coalesce(%s, now()))",
                  (oid, t, self.rng.randint(a, b), total, created, created))
        for i, ((p, pr), q) in enumerate(lines, 1):
            c.execute("INSERT INTO order_items VALUES (%s,%s,%s,%s,%s,%s)", (oid, i, t, p, q, pr))

    def op(self):
        r = self.rng.random()
        with self.conn.cursor() as c:
            if r < 0.50:
                kind = "insert"
                self.insert_order(c)
            elif r < 0.55:
                kind = "insert_past"  # late-arriving order with a timestamp in the past (µs precision)
                span = (self.t_max - self.t_min).total_seconds()
                self.insert_order(c, self.t_min + timedelta(seconds=self.rng.uniform(0, span)))
            elif r < 0.75:
                kind = "advance_recent"
                row = self._random_order(c, recent=True)
                if row:
                    c.execute("UPDATE orders SET status=%s, updated_at=now() WHERE order_id=%s",
                              (self.rng.choice(NEXT[row[2]]), row[0]))
            elif r < 0.88:
                kind = "change_old"  # e.g. a 60-day-old order gets cancelled / returned
                row = self._random_order(c)
                if row:
                    c.execute("UPDATE orders SET status=%s, updated_at=now() WHERE order_id=%s",
                              (self.rng.choice(NEXT[row[2]]), row[0]))
            elif r < 0.92:
                kind = "delete"
                row = self._random_order(c)
                if row:
                    c.execute("DELETE FROM order_items WHERE order_id=%s", (row[0],))
                    c.execute("DELETE FROM orders WHERE order_id=%s", (row[0],))
            elif r < 0.96:
                kind = "add_item"
                row = self._random_order(c)
                if row:
                    p, pr = self.rng.choice(self.products[row[1]])
                    q = self.rng.randint(1, 4)
                    c.execute("INSERT INTO order_items SELECT %s, coalesce(max(line_no),0)+1, %s, %s, %s, %s "
                              "FROM order_items WHERE order_id=%s", (row[0], row[1], p, q, pr, row[0]))
                    c.execute("UPDATE orders SET total_cents = total_cents + %s, updated_at=now() WHERE order_id=%s",
                              (q * pr, row[0]))
            elif r < 0.98:
                kind = "remove_item"
                row = self._random_order(c)
                if row:
                    c.execute("DELETE FROM order_items WHERE (order_id, line_no) = "
                              "(SELECT order_id, max(line_no) FROM order_items WHERE order_id=%s GROUP BY 1) "
                              "RETURNING qty::bigint * price_cents", (row[0],))
                    got = c.fetchone()
                    if got:
                        c.execute("UPDATE orders SET total_cents = total_cents - %s, updated_at=now() "
                                  "WHERE order_id=%s", (got[0], row[0]))
            else:
                kind = "backdate"  # created_at itself changes: moves the order across buckets
                row = self._random_order(c)
                if row:
                    c.execute("UPDATE orders SET created_at = created_at - make_interval(secs => %s) "
                              "WHERE order_id=%s", (self.rng.uniform(1, 10 * 86400), row[0]))
        self.conn.commit()
        self.counts[kind] = self.counts.get(kind, 0) + 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="host=localhost port=5433 user=zeta dbname=zeta")
    ap.add_argument("--ops", type=int, default=0, help="run this many transactions as fast as possible")
    ap.add_argument("--rate", type=float, default=10, help="tx/s for --seconds mode")
    ap.add_argument("--seconds", type=float, default=0)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    s = Store(a.dsn, a.seed)
    t0 = time.time()
    if a.ops:
        for _ in range(a.ops):
            s.op()
    else:
        i = 0
        while time.time() - t0 < a.seconds:
            s.op()
            i += 1
            time.sleep(max(0.0, t0 + i / a.rate - time.time()))
    print(f"{sum(s.counts.values())} tx in {time.time() - t0:.1f}s: {s.counts}")


if __name__ == "__main__":
    main()
