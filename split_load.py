#!/usr/bin/env python3
"""Load generated CSVs as one Postgres database per store (tenant).

  python split_load.py data_1m  --prefix t1m      # 10 stores  -> databases t1m_001 .. t1m_010
  python split_load.py data_10m --prefix t10m     # 100 stores -> databases t10m_001 .. t10m_100
  python split_load.py data_10m --prefix g10m --dbs 20   # optional: 100 stores packed into 20 databases

Every database gets the organisers' schema.sql and only its own stores' rows. ZETA discovers the
databases by prefix and follows one change stream per database.
"""
import argparse
import io
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import psycopg2
import pyarrow as pa
import pyarrow.csv as pacsv

TABLES = ["tenants", "customers", "products", "orders", "order_items"]
BASE = "host=localhost port=5433 user=zeta dbname={}"


def read(path):
    """Read a CSV keeping every column as text (exact round trip), plus tenant_id as int for splitting."""
    head = pacsv.open_csv(path).schema.names
    tbl = pacsv.read_csv(path, convert_options=pacsv.ConvertOptions(
        column_types={c: (pa.int64() if c == "tenant_id" else pa.string()) for c in head},
        strings_can_be_null=False))
    return tbl


def db_names(prefix, tids, n_dbs):
    if n_dbs is None or n_dbs >= len(tids):
        return {t: f"{prefix}_{t:03d}" for t in tids}
    # contiguous groups keep the big stores (low ids) spread across the first databases
    return {t: f"{prefix}_g{(i * n_dbs) // len(tids) + 1:02d}" for i, t in enumerate(sorted(tids))}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("data_dir")
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--dbs", type=int, default=None, help="pack stores into this many databases (default: one per store)")
    ap.add_argument("--workers", type=int, default=6)
    a = ap.parse_args()
    t0 = time.time()

    tables = {t: read(Path(a.data_dir) / f"{t}.csv") for t in TABLES}
    tids = sorted(set(tables["tenants"]["tenant_id"].to_pylist()))
    names = db_names(a.prefix, tids, a.dbs)
    print(f"read {a.data_dir} in {time.time() - t0:.1f}s: {len(tids)} stores -> {len(set(names.values()))} databases")

    # row ranges per store, per table (stable sort by tenant_id; generator output is already grouped)
    parts = {}
    for t, tbl in tables.items():
        tid_col = tbl["tenant_id"].to_numpy()
        order = np.argsort(tid_col, kind="stable")
        sorted_tids = tid_col[order]
        parts[t] = (tbl, order, sorted_tids)

    schema = (Path(__file__).parent / "schema.sql").read_text()
    admin = psycopg2.connect(BASE.format("postgres"))
    admin.autocommit = True
    with admin.cursor() as c:
        c.execute("SELECT datname FROM pg_database WHERE datname LIKE %s", (a.prefix + "\\_%",))
        for (old,) in c.fetchall():
            c.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                      "WHERE database = %s AND NOT active", (old,))
            c.execute(f'DROP DATABASE "{old}"')
        for db in sorted(set(names.values())):
            c.execute(f'CREATE DATABASE "{db}"')
    admin.close()

    by_db = {}
    for tid, db in names.items():
        by_db.setdefault(db, []).append(tid)

    def load(db):
        conn = psycopg2.connect(BASE.format(db))
        rows = {}
        with conn.cursor() as c:
            c.execute(schema)
            for t in TABLES:
                tbl, order, sorted_tids = parts[t]
                idx = np.concatenate([order[np.searchsorted(sorted_tids, tid, "left"):
                                            np.searchsorted(sorted_tids, tid, "right")] for tid in by_db[db]])
                sub = tbl.take(pa.array(np.sort(idx)))
                buf = io.BytesIO()
                pacsv.write_csv(sub, buf)
                buf.seek(0)
                c.copy_expert(f"COPY {t} FROM STDIN WITH (FORMAT csv, HEADER true)", buf)
                rows[t] = sub.num_rows
        conn.commit()
        conn.autocommit = True
        with conn.cursor() as c:
            c.execute("ANALYZE")
        conn.close()
        return db, rows

    total = {t: 0 for t in TABLES}
    with ThreadPoolExecutor(a.workers) as ex:
        for db, rows in ex.map(load, sorted(by_db)):
            for t in TABLES:
                total[t] += rows[t]
            print(f"  {db}: stores {by_db[db]} · {rows['orders']:,} orders · {rows['order_items']:,} items")
    print(f"loaded {len(by_db)} databases in {time.time() - t0:.1f}s · totals: " +
          ", ".join(f"{t} {n:,}" for t, n in total.items()))


if __name__ == "__main__":
    main()
