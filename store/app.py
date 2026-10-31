"""Demo store app: the customer side of the platform.

Customers pick a shop (tenant), put products in a cart, place orders and cancel them.
Every shop has its own Postgres database; the app writes to that shop's database exactly like
a real shop backend would. It never talks to ZETA: ZETA sees these orders only through each
database's change stream.

  ./run_store.sh t10m       # http://<this laptop's IP>:9000/?shop=7
"""
import os
import random
from pathlib import Path
from typing import List

import psycopg2
import psycopg2.errors
import psycopg2.pool
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

PG = os.environ.get("STORE_PG", "host=localhost port=5433 user=zeta")
DBS = os.environ.get("STORE_DBS", "t10m_*")
app = FastAPI(title="ZETA demo store")


def _discover():
    c = psycopg2.connect(f"{PG} dbname=postgres")
    with c.cursor() as cur:
        if "*" in DBS:
            cur.execute("SELECT datname FROM pg_database WHERE datname LIKE %s ORDER BY 1",
                        (DBS.replace("_", "\\_").replace("*", "%"),))
            dbs = [r[0] for r in cur.fetchall()]
        else:
            dbs = DBS.split(",")
    c.close()
    shops, pools = {}, {}
    for db in dbs:
        pools[db] = psycopg2.pool.ThreadedConnectionPool(1, 4, f"{PG} dbname={db}")
        conn = pools[db].getconn()
        with conn.cursor() as cur:
            cur.execute("SELECT tenant_id, name, tier FROM tenants")
            for tid, name, tier in cur.fetchall():
                shops[tid] = {"tenant_id": tid, "name": name, "tier": tier, "database": db}
        conn.rollback()
        pools[db].putconn(conn)
    return shops, pools


SHOPS, POOLS = _discover()


def run(tid, fn):
    """Run fn(cursor) in the shop's own database."""
    if tid not in SHOPS:
        raise HTTPException(404, f"no shop {tid}")
    pool = POOLS[SHOPS[tid]["database"]]
    conn = pool.getconn()
    try:
        with conn.cursor() as c:
            out = fn(c)
        conn.commit()
        return out
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def setup(c):
    # order ids for the store; a conflict with another writer just advances it (see place_order)
    c.execute("CREATE SEQUENCE IF NOT EXISTS store_order_id_seq")
    c.execute("SELECT setval('store_order_id_seq', greatest((SELECT max(order_id) FROM orders), "
              "(SELECT last_value FROM store_order_id_seq)))")


for _tid in SHOPS:
    run(_tid, setup)


class Line(BaseModel):
    product_id: int
    qty: int = Field(ge=1, le=20)


class NewOrder(BaseModel):
    customer_id: int
    items: List[Line] = Field(min_length=1, max_length=20)


@app.get("/", response_class=HTMLResponse)
def page():
    return (Path(__file__).parent / "index.html").read_text()


@app.get("/api/shops")
def shops():
    return [SHOPS[t] for t in sorted(SHOPS)]


@app.get("/api/shops/{tid}/catalog")
def catalog(tid: int, n: int = 12):
    def q(c):
        c.execute("SELECT product_id, name, category, price_cents FROM products WHERE tenant_id = %s "
                  "ORDER BY random() LIMIT %s", (tid, min(n, 48)))
        return c.fetchall()
    rows = run(tid, q)
    return [{"product_id": p, "name": nm, "category": cat, "price_cents": pr} for p, nm, cat, pr in rows]


@app.get("/api/shops/{tid}/customer")
def random_customer(tid: int):
    """A real customer of this shop (picked among recent buyers via the (tenant, created_at) index)."""
    def q(c):
        c.execute("SELECT customer_id FROM orders WHERE tenant_id = %s ORDER BY created_at DESC LIMIT 200", (tid,))
        ids = [r[0] for r in c.fetchall()]
        if not ids:
            return None
        c.execute("SELECT customer_id, name, city FROM customers WHERE customer_id = %s", (random.choice(ids),))
        return c.fetchone()
    row = run(tid, q)
    if row is None:
        raise HTTPException(404, f"no customers for shop {tid}")
    return {"customer_id": row[0], "name": row[1], "city": row[2]}


@app.post("/api/shops/{tid}/orders")
def place_order(tid: int, body: NewOrder):
    pids = [l.product_id for l in body.items]

    def q(c):
        c.execute("SELECT 1 FROM customers WHERE customer_id = %s AND tenant_id = %s", (body.customer_id, tid))
        if c.fetchone() is None:
            raise HTTPException(400, "customer does not belong to this shop")
        c.execute("SELECT product_id, price_cents FROM products WHERE tenant_id = %s AND product_id = ANY(%s)",
                  (tid, pids))
        price = dict(c.fetchall())
        missing = set(pids) - set(price)
        if missing:
            raise HTTPException(400, f"products not sold by this shop: {sorted(missing)}")
        total = sum(price[l.product_id] * l.qty for l in body.items)
        for _ in range(5):
            c.execute("SELECT nextval('store_order_id_seq')")
            oid = c.fetchone()[0]
            c.execute("SAVEPOINT ins")
            try:
                c.execute("INSERT INTO orders VALUES (%s,%s,%s,'placed',%s,now(),now()) RETURNING created_at",
                          (oid, tid, body.customer_id, total))
                created = c.fetchone()[0]
                break
            except psycopg2.errors.UniqueViolation:  # another writer used this id: skip ahead
                c.execute("ROLLBACK TO SAVEPOINT ins")
                c.execute("SELECT setval('store_order_id_seq', (SELECT max(order_id) FROM orders))")
        else:
            raise HTTPException(503, "could not allocate an order id")
        for n, l in enumerate(body.items, 1):
            c.execute("INSERT INTO order_items VALUES (%s,%s,%s,%s,%s,%s)",
                      (oid, n, tid, l.product_id, l.qty, price[l.product_id]))
        return oid, total, created

    oid, total, created = run(tid, q)
    return {"order_id": oid, "tenant_id": tid, "status": "placed", "total_cents": total,
            "created_at": created.isoformat()}


@app.get("/api/shops/{tid}/orders")
def get_orders(tid: int, ids: str):
    oids = [int(x) for x in ids.split(",") if x.strip().isdigit()][:50]

    def q(c):
        c.execute("SELECT order_id, tenant_id, status, total_cents, created_at, updated_at FROM orders "
                  "WHERE order_id = ANY(%s)", (oids,))
        orders = {r[0]: r for r in c.fetchall()}
        c.execute("SELECT i.order_id, i.qty, p.name, i.price_cents FROM order_items i "
                  "JOIN products p ON p.product_id = i.product_id WHERE i.order_id = ANY(%s) "
                  "ORDER BY i.order_id, i.line_no", (oids,))
        items = {}
        for oid, qty, name, pr in c.fetchall():
            items.setdefault(oid, []).append({"qty": qty, "name": name, "price_cents": pr})
        return orders, items

    orders, items = run(tid, q)
    return [{"order_id": o, "tenant_id": r[1], "status": r[2], "total_cents": r[3], "created_at": r[4].isoformat(),
             "updated_at": r[5].isoformat(), "items": items.get(o, [])}
            for o in oids if (r := orders.get(o))]


@app.post("/api/shops/{tid}/orders/{oid}/cancel")
def cancel(tid: int, oid: int):
    def q(c):
        c.execute("UPDATE orders SET status = 'cancelled', updated_at = now() WHERE order_id = %s "
                  "AND tenant_id = %s AND status IN ('placed', 'shipped') RETURNING status, updated_at",
                  (oid, tid))
        return c.fetchone()
    row = run(tid, q)
    if row is None:
        raise HTTPException(409, "order not found in this shop, or already delivered/cancelled")
    return {"order_id": oid, "status": row[0], "cancelled_at": row[1].isoformat()}
