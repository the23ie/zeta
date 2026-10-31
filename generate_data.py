#!/usr/bin/env python3
"""
ZETA dataset generator — Shopify-like multi-tenant orders/products/customers.

Matches the PRD:
  * small run : --orders 1000000  --tenants 10
  * main run  : --orders 10000000 --tenants 100   (default)
  * skewed tenants ("a few tenants are much bigger than the rest"): Zipf(s) sizes
  * realistic time shape: growth trend + weekly + diurnal seasonality
  * order lifecycle statuses consistent with order age (placed/shipped/delivered/cancelled/returned)
  * Zipf product popularity inside each tenant, so "Top-10 products" is meaningful

Output: CSVs in --out, loadable with load.sql (\\copy). Deterministic for a given --seed.

Usage:
  python generate_data.py --orders 1000000 --tenants 10 --out data_1m
  python generate_data.py --out data_10m
"""
import argparse
import os
import time
from datetime import datetime, timezone

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv

STATUSES = np.array(["placed", "shipped", "delivered", "cancelled", "returned"])
CATEGORIES = np.array(["apparel", "electronics", "home", "beauty", "grocery",
                       "sports", "toys", "books", "footwear", "accessories"])
CITIES = np.array(["Bengaluru", "Mumbai", "Delhi", "Chennai", "Hyderabad", "Pune",
                   "Kolkata", "Ahmedabad", "Jaipur", "Kochi", "Lucknow", "Tirupati"])
# Diurnal profile (IST-ish shape): quiet at night, lunch bump, evening peak
DIURNAL = np.array([2, 1, 1, 1, 1, 2, 3, 5, 7, 8, 9, 10, 11, 10, 9, 9, 10, 11,
                    13, 15, 16, 14, 9, 5], dtype=float)
WEEKLY = np.array([1.0, 0.95, 0.95, 1.0, 1.1, 1.3, 1.25])  # Mon..Sun


def zipf_weights(n, s):
    w = 1.0 / np.arange(1, n + 1) ** s
    return w / w.sum()


def tenant_sizes(total, n_tenants, s):
    """Zipf-distributed tenant sizes that sum exactly to total; every tenant gets >= 1000 orders."""
    w = zipf_weights(n_tenants, s)
    floor = min(1000, total // (n_tenants * 2))
    sizes = np.floor(w * (total - floor * n_tenants)).astype(np.int64) + floor
    sizes[0] += total - sizes.sum()
    return sizes


def write_csv(table: pa.Table, path: str, first: bool):
    opts = pacsv.WriteOptions(include_header=first)
    with open(path, "ab" if not first else "wb") as f:
        pacsv.write_csv(table, f, write_options=opts)


def sample_timestamps(rng, n, start_s, days):
    """Seconds since epoch with growth trend, weekly and diurnal seasonality."""
    day_idx = np.arange(days)
    start_dow = datetime.fromtimestamp(start_s, tz=timezone.utc).weekday()
    day_w = (1.0 + 0.6 * day_idx / max(days - 1, 1)) * WEEKLY[(start_dow + day_idx) % 7]
    day_w /= day_w.sum()
    d = rng.choice(days, size=n, p=day_w)
    h = rng.choice(24, size=n, p=DIURNAL / DIURNAL.sum())
    sec = rng.integers(0, 3600, size=n)
    return start_s + d * 86400 + h * 3600 + sec


def assign_status(rng, age_s):
    n = age_s.size
    u = rng.random(n)
    st = np.empty(n, dtype=np.int8)
    young = age_s < 86400                      # < 1 day
    mid = (age_s >= 86400) & (age_s < 5 * 86400)
    old = age_s >= 5 * 86400
    # cumulative probability tables: placed, shipped, delivered, cancelled, returned
    for mask, p in ((young, [0.70, 0.22, 0.00, 0.08, 0.00]),
                    (mid,   [0.10, 0.60, 0.22, 0.08, 0.00]),
                    (old,   [0.00, 0.01, 0.86, 0.08, 0.05])):
        st[mask] = np.searchsorted(np.cumsum(p), u[mask], side="right").clip(0, 4)
    return st


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=10_000_000)
    ap.add_argument("--tenants", type=int, default=100)
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--tenant-skew", type=float, default=1.1, help="Zipf s for tenant sizes")
    ap.add_argument("--product-skew", type=float, default=1.2, help="Zipf s for product popularity")
    ap.add_argument("--end", type=str, default=None, help="ISO end time (default: now, UTC, rounded to hour)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", type=str, default="data")
    a = ap.parse_args()

    rng = np.random.default_rng(a.seed)
    os.makedirs(a.out, exist_ok=True)
    end = (datetime.fromisoformat(a.end) if a.end else datetime.now(timezone.utc))
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    end_s = int(end.timestamp()) // 3600 * 3600
    start_s = end_s - a.days * 86400

    sizes = tenant_sizes(a.orders, a.tenants, a.tenant_skew)
    tiers = np.where(sizes >= np.quantile(sizes, 0.95), "enterprise",
                     np.where(sizes >= np.quantile(sizes, 0.6), "growth", "starter"))
    tenants = pa.table({"tenant_id": np.arange(1, a.tenants + 1, dtype=np.int32),
                        "name": [f"store_{i:03d}" for i in range(1, a.tenants + 1)],
                        "tier": tiers})
    write_csv(tenants, f"{a.out}/tenants.csv", True)

    paths = {k: f"{a.out}/{k}.csv" for k in ("customers", "products", "orders", "order_items")}
    next_cust = next_prod = next_order = 1
    n_items_total = 0
    t0 = time.time()

    for ti, n_orders in enumerate(sizes):
        tid = ti + 1
        first = ti == 0
        n_orders = int(n_orders)

        # --- products (Zipf popularity inside the tenant) ---
        n_prod = int(np.clip(np.sqrt(n_orders) * 2, 20, 5000))
        prod_ids = np.arange(next_prod, next_prod + n_prod, dtype=np.int64)
        next_prod += n_prod
        prices = (np.exp(rng.normal(6.5, 1.0, n_prod)).clip(50, 50000) * 100).astype(np.int64)  # cents
        write_csv(pa.table({
            "product_id": prod_ids, "tenant_id": np.full(n_prod, tid, np.int32),
            "name": [f"t{tid}_sku_{j}" for j in range(n_prod)],
            "category": rng.choice(CATEGORIES, n_prod), "price_cents": prices}),
            paths["products"], first)
        pop_cdf = np.cumsum(zipf_weights(n_prod, a.product_skew)[rng.permutation(n_prod)])

        # --- customers (repeat buyers: ~4 orders/customer, Zipf-ish activity) ---
        n_cust = max(50, n_orders // 4)
        cust_ids = np.arange(next_cust, next_cust + n_cust, dtype=np.int64)
        next_cust += n_cust
        cust_created = start_s - rng.integers(0, 365 * 86400, n_cust)
        write_csv(pa.table({
            "customer_id": cust_ids, "tenant_id": np.full(n_cust, tid, np.int32),
            "name": [f"cust_{c}" for c in cust_ids], "city": rng.choice(CITIES, n_cust),
            "created_at": pa.array(cust_created * 1_000_000, pa.timestamp("us", tz="UTC"))}),
            paths["customers"], first)
        cust_cdf = np.cumsum(zipf_weights(n_cust, 0.6)[rng.permutation(n_cust)])

        # --- orders ---
        order_ids = np.arange(next_order, next_order + n_orders, dtype=np.int64)
        next_order += n_orders
        created = np.sort(sample_timestamps(rng, n_orders, start_s, a.days))
        status = assign_status(rng, end_s - created)
        updated = np.minimum(created + rng.integers(60, 3 * 86400, n_orders) * (status > 0), end_s)
        cust = cust_ids[np.searchsorted(cust_cdf, rng.random(n_orders)).clip(0, n_cust - 1)]

        # --- order items: 1 + Poisson(0.6) lines, qty 1..k ---
        n_lines = (1 + rng.poisson(0.6, n_orders)).clip(1, 6)
        item_order = np.repeat(order_ids, n_lines)
        line_no = (np.arange(item_order.size) - np.repeat(np.cumsum(n_lines) - n_lines, n_lines) + 1).astype(np.int16)
        pidx = np.searchsorted(pop_cdf, rng.random(item_order.size)).clip(0, n_prod - 1)
        qty = rng.geometric(0.7, item_order.size).clip(1, 10).astype(np.int32)
        line_price = prices[pidx]
        totals = np.bincount(np.repeat(np.arange(n_orders), n_lines),
                             weights=(qty * line_price).astype(np.float64), minlength=n_orders).astype(np.int64)
        n_items_total += item_order.size

        write_csv(pa.table({
            "order_id": order_ids, "tenant_id": np.full(n_orders, tid, np.int32),
            "customer_id": cust, "status": STATUSES[status], "total_cents": totals,
            "created_at": pa.array(created * 1_000_000, pa.timestamp("us", tz="UTC")),
            "updated_at": pa.array(updated * 1_000_000, pa.timestamp("us", tz="UTC"))}),
            paths["orders"], first)
        write_csv(pa.table({
            "order_id": item_order, "line_no": line_no,
            "tenant_id": np.full(item_order.size, tid, np.int32),
            "product_id": prod_ids[pidx], "qty": qty, "price_cents": line_price}),
            paths["order_items"], first)

    top3 = sizes[:3].sum() / sizes.sum() * 100
    print(f"done in {time.time() - t0:.1f}s -> {a.out}/")
    print(f"tenants={a.tenants} orders={sizes.sum():,} items={n_items_total:,} "
          f"products={next_prod - 1:,} customers={next_cust - 1:,}")
    print(f"skew: largest tenant={sizes[0]:,} orders, smallest={sizes[-1]:,}, top-3 share={top3:.1f}%")
    print(f"time range: {datetime.fromtimestamp(start_s, timezone.utc)} -> {datetime.fromtimestamp(end_s, timezone.utc)}")


if __name__ == "__main__":
    main()
