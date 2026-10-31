#!/usr/bin/env python3
"""Store-app write traffic across the store databases (one Postgres database per store):
new orders, status changes, cancellations of old orders, returns, deletes, items added to
existing orders, backdated created_at.

  python simulate.py --dbs 't1m_*' --ops 500                 # burst of 500 transactions
  python simulate.py --dbs 't10m_*' --rate 20 --seconds 600  # steady 20 tx/s for 10 minutes
"""
import argparse
import os
import random
import time
from datetime import timedelta

import psycopg2

from zeta.cdc import discover

NEXT = {"placed": ["shipped", "cancelled"], "shipped": ["delivered", "cancelled"],
        "delivered": ["returned"], "cancelled": ["placed"], "returned": ["delivered"]}
PG = os.environ.get("ZETA_PG", "host=localhost port=5433 user=zeta")


class Shop:
    """One store database, opened on first use."""

    def __init__(self, db):
        self.conn = psycopg2.connect(f"{PG} dbname={db}")
        c = self.conn.cursor()
        c.execute("SELECT tenant_id FROM tenants")
        self.tid = c.fetchone()[0]
        c.execute("SELECT product_id, price_cents FROM products")
        self.products = c.fetchall()
        c.execute("SELECT min(customer_id), max(customer_id) FROM customers")
        self.customers = c.fetchone()
        c.execute("SELECT min(order_id), max(order_id), min(created_at), max(created_at) FROM orders")
        self.min_oid, self.max_oid, self.t_min, self.t_max = c.fetchone()
        self.conn.commit()


class Store:
    def __init__(self, dbs, seed):
        self.rng = random.Random(seed)
        self.dbs = dbs
        self.shops = {}
        self.counts = {}

    def _shop(self):
        # skewed like the data: the first (biggest) stores get most of the traffic
        db = self.dbs[min(int(self.rng.paretovariate(1.2)) - 1, len(self.dbs) - 1)]
        if db not in self.shops:
            self.shops[db] = Shop(db)
        return self.shops[db]

    def _random_order(self, s, c, recent=False):
        lo = max(s.min_oid, s.max_oid - 5000) if recent else s.min_oid  # each store has its own id range
        c.execute("SELECT order_id, status FROM orders WHERE order_id >= %s ORDER BY order_id LIMIT 1 FOR UPDATE",
                  (self.rng.randint(lo, s.max_oid),))
        return c.fetchone()

    def insert_order(self, s, c, created=None):
        a, b = s.customers
        s.max_oid += 1
        lines = [(self.rng.choice(s.products), self.rng.randint(1, 3)) for _ in range(self.rng.randint(1, 3))]
        total = sum(pr * q for (_, pr), q in lines)
        c.execute("INSERT INTO orders VALUES (%s,%s,%s,'placed',%s,coalesce(%s, now()),coalesce(%s, now()))",
                  (s.max_oid, s.tid, self.rng.randint(a, b), total, created, created))
        for i, ((p, pr), q) in enumerate(lines, 1):
            c.execute("INSERT INTO order_items VALUES (%s,%s,%s,%s,%s,%s)", (s.max_oid, i, s.tid, p, q, pr))

    def op(self):
        s = self._shop()
        r = self.rng.random()
        with s.conn.cursor() as c:
            if r < 0.50:
                kind = "insert"
                self.insert_order(s, c)
            elif r < 0.55:
                kind = "insert_past"  # late-arriving order with a timestamp in the past (µs precision)
                span = (s.t_max - s.t_min).total_seconds()
                self.insert_order(s, c, s.t_min + timedelta(seconds=self.rng.uniform(0, span)))
            elif r < 0.75:
                kind = "advance_recent"
                row = self._random_order(s, c, recent=True)
                if row:
                    c.execute("UPDATE orders SET status=%s, updated_at=now() WHERE order_id=%s",
                              (self.rng.choice(NEXT[row[1]]), row[0]))
            elif r < 0.88:
                kind = "change_old"  # e.g. a 60-day-old order gets cancelled / returned
                row = self._random_order(s, c)
                if row:
                    c.execute("UPDATE orders SET status=%s, updated_at=now() WHERE order_id=%s",
                              (self.rng.choice(NEXT[row[1]]), row[0]))
            elif r < 0.92:
                kind = "delete"
                row = self._random_order(s, c)
                if row:
                    c.execute("DELETE FROM order_items WHERE order_id=%s", (row[0],))
                    c.execute("DELETE FROM orders WHERE order_id=%s", (row[0],))
            elif r < 0.96:
                kind = "add_item"
                row = self._random_order(s, c)
                if row:
                    p, pr = self.rng.choice(s.products)
                    q = self.rng.randint(1, 4)
                    c.execute("INSERT INTO order_items SELECT %s, coalesce(max(line_no),0)+1, %s, %s, %s, %s "
                              "FROM order_items WHERE order_id=%s", (row[0], s.tid, p, q, pr, row[0]))
                    c.execute("UPDATE orders SET total_cents = total_cents + %s, updated_at=now() WHERE order_id=%s",
                              (q * pr, row[0]))
            elif r < 0.98:
                kind = "remove_item"
                row = self._random_order(s, c)
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
                row = self._random_order(s, c)
                if row:
                    c.execute("UPDATE orders SET created_at = created_at - make_interval(secs => %s) "
                              "WHERE order_id=%s", (self.rng.uniform(1, 10 * 86400), row[0]))
        s.conn.commit()
        self.counts[kind] = self.counts.get(kind, 0) + 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dbs", default="t1m_*", help="store databases: prefix pattern or comma list")
    ap.add_argument("--ops", type=int, default=0, help="run this many transactions as fast as possible")
    ap.add_argument("--rate", type=float, default=10, help="tx/s for --seconds mode")
    ap.add_argument("--seconds", type=float, default=0)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    s = Store(discover(PG, a.dbs), a.seed)
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
    print(f"{sum(s.counts.values())} tx across {len(s.shops)} store databases in {time.time() - t0:.1f}s: {s.counts}")


if __name__ == "__main__":
    main()
