"""ZETA delta engine.

Per tenant:
  * hourly Fenwick over vectors [count|revenue] x status            (FR-1, FR-2, FR-4)
  * daily Fenwick over dense product-sales vectors                   (FR-3)
  * an in-memory columnar mirror of orders/items, sorted by created_at,
    used only for the two partial-bucket "edges" of a range query and for
    join state (which items belong to an order when its status changes).

Every change is applied as a Z-set delta: INSERT -> +new, DELETE -> -old,
UPDATE -> -old +new. All aggregates are sums, so retractions are exact.
Time is int64 microseconds since the Unix epoch, buckets are UTC-aligned.
"""
import bisect
import hashlib
import io
import math
import threading
import time
from collections import deque

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv

STATUSES = ("placed", "shipped", "delivered", "cancelled", "returned")
S_IDX = {s: i for i, s in enumerate(STATUSES)}
NS = len(STATUSES)
DELETED = NS  # mirror-only marker: base row moved to overflow or deleted
# statuses whose items count toward product sales (index DELETED is never counted)
COUNTED = np.array([True, True, True, False, False, False])
HOUR = 3_600_000_000
DAY = 24 * HOUR

# Homomorphic ledger hash: H = sum h(order_id|status|total_cents|epoch_seconds) mod p
P61 = (1 << 61) - 1
HASH_SQL = ("('x' || substr(md5(order_id || '|' || status || '|' || total_cents || '|' || "
            "extract(epoch FROM created_at)::bigint), 1, 15))::bit(60)::bigint")


def _rsec(us):
    """extract(epoch)::bigint rounds half away from zero."""
    q, r = divmod(abs(us), 1_000_000)
    q += r >= 500_000
    return q if us >= 0 else -q


def row_hash(oid, status, total, created_us):
    return int(hashlib.md5(f"{oid}|{status}|{total}|{_rsec(created_us)}".encode()).hexdigest()[:15], 16)


class Fenwick:
    """Binary indexed tree over axis 0 of an int64 array; each node may be a vector."""

    def __init__(self, raw):
        n = raw.shape[0]
        tree = raw.astype(np.int64, copy=True)
        for i in range(1, n + 1):
            j = i + (i & -i)
            if j <= n:
                tree[j - 1] += tree[i - 1]
        self.n, self.tree = n, tree

    def add(self, i, delta):
        i += 1
        while i <= self.n:
            self.tree[i - 1] += delta
            i += i & -i

    def prefix(self, i):
        s = np.zeros(self.tree.shape[1:], np.int64)
        while i > 0:
            s += self.tree[i - 1]
            i -= i & -i
        return s

    def range(self, a, b):
        return self.prefix(b) - self.prefix(a)

    def add_point(self, i, idx, delta):
        """Add a scalar to one element (idx) of node vector i: O(log n), no full-width vector."""
        i += 1
        while i <= self.n:
            self.tree[(i - 1,) + idx] += delta
            i += i & -i

    def grow_cols(self, extra):
        self.tree = np.pad(self.tree, ((0, 0), (0, extra)))

    @classmethod
    def from_tree(cls, tree):
        """Rebuild the object around an already-built tree (from a snapshot): no O(n) build."""
        f = cls.__new__(cls)
        f.n, f.tree = tree.shape[0], tree
        return f

    def to_raw(self):
        """Inverse of the build: recover the per-bucket array from the tree."""
        r = self.tree.copy()
        for i in range(self.n, 0, -1):
            j = i + (i & -i)
            if j <= self.n:
                r[j - 1] -= r[i - 1]
        return r


def _ceil_div(a, b):
    return -(-a // b)


class Tenant:
    def __init__(self, tid, epoch, T):
        self.tid = tid
        self.epoch, self.T, self.D = epoch, T, T // 24
        self.hour_raw = np.zeros((T, 2, NS), np.int64)
        self.hour = Fenwick(self.hour_raw)
        self.hb = np.zeros(T, np.int64)  # ledger hash per hour bucket (mod P61)
        self.h_out = 0                   # ledger hash of orders outside the bucket axis
        self.pid_arr = np.zeros(0, np.int64)
        self.pid_names = []
        self.pid_index = {}
        self.pid_cat = np.zeros(0, np.int16)  # product category code (Engine.categories)
        self.day = Fenwick(np.zeros((self.D, 0), np.int64))
        self.owner = None  # OwnerIndex this store reports its deltas to (zeta/owner.py)
        self.g_sidx, self.g_index, self.g_pmap = None, None, None
        # base mirror (sorted by created_us); rows are never inserted, only flagged DELETED
        self.b_created = np.zeros(0, np.int64)
        self.b_oid = np.zeros(0, np.int64)
        self.b_status = np.zeros(0, np.int8)
        self.b_total = np.zeros(0, np.int64)
        self.b_ptr = np.zeros(1, np.int64)
        self.i_pidx = np.zeros(0, np.int64)
        self.i_sales = np.zeros(0, np.int64)
        self.i_line = np.zeros(0, np.int16)
        self.b_oid_sorted = np.zeros(0, np.int64)
        self.b_oid_pos = np.zeros(0, np.int64)
        # overflow mirror: orders inserted (or moved) after bootstrap
        self.ov = {}        # oid -> [created_us, status, total, {line: (pidx, sales)}]
        self.ov_keys = []   # sorted [(created_us, oid)]

    # ---------- products ----------
    def set_products(self, pids, names, cats=None):
        self.pid_arr = np.asarray(pids, np.int64)
        self.pid_names = list(names)
        self.pid_index = {int(p): i for i, p in enumerate(self.pid_arr)}
        self.pid_cat = np.asarray(cats if cats is not None else [0] * len(self.pid_arr), np.int16)

    def pidx_of(self, pid, name=None):
        i = self.pid_index.get(pid)
        if i is None:
            i = len(self.pid_arr)
            self.pid_arr = np.append(self.pid_arr, np.int64(pid))
            self.pid_names.append(name or f"product_{pid}")
            self.pid_index[pid] = i
            self.pid_cat = np.append(self.pid_cat, np.int16(0))
            self.day.grow_cols(1)
            if self.owner is not None:
                self.owner.add_product(self, i)
        elif name is not None:
            self.pid_names[i] = name
        return i

    @property
    def P(self):
        return len(self.pid_arr)

    # ---------- mirror ----------
    def find(self, oid):
        o = self.ov.get(oid)
        if o is not None:
            return o, None
        k = np.searchsorted(self.b_oid_sorted, oid)
        if k < len(self.b_oid_sorted) and self.b_oid_sorted[k] == oid:
            pos = int(self.b_oid_pos[k])
            if self.b_status[pos] != DELETED:
                return None, pos
        return None, None

    def base_items(self, pos):
        a, b = self.b_ptr[pos], self.b_ptr[pos + 1]
        return list(zip(self.i_pidx[a:b].tolist(), self.i_sales[a:b].tolist()))

    def promote(self, pos):
        """Move a base row to the overflow mirror so its items/created_at can change."""
        a, b = self.b_ptr[pos], self.b_ptr[pos + 1]
        items = {int(l): (int(p), int(s)) for l, p, s in
                 zip(self.i_line[a:b], self.i_pidx[a:b], self.i_sales[a:b])}
        o = [int(self.b_created[pos]), int(self.b_status[pos]), int(self.b_total[pos]), items]
        self.set_base(pos, status=DELETED)
        self.ov_add(int(self.b_oid[pos]), o)
        return o

    def set_base(self, pos, status=None, total=None):
        if status is not None:
            self.b_status[pos] = status
        if total is not None:
            self.b_total[pos] = total
        if self.owner is not None:
            self.owner.base_set(self, pos, status, total)

    def ov_add(self, oid, o):
        self.ov[oid] = o
        bisect.insort(self.ov_keys, (o[0], oid))
        if self.owner is not None:
            self.owner.ov_add(self, o[0], oid)

    def ov_remove(self, oid):
        o = self.ov.pop(oid)
        k = bisect.bisect_left(self.ov_keys, (o[0], oid))
        del self.ov_keys[k]
        if self.owner is not None:
            self.owner.ov_remove(self, o[0], oid)
        return o

    def _ov_range(self, a, b):
        lo = bisect.bisect_left(self.ov_keys, (a,))
        hi = bisect.bisect_left(self.ov_keys, (b,))
        return [self.ov[oid] for _, oid in self.ov_keys[lo:hi]]

    # ---------- Z-set deltas into the aggregates ----------
    def hour_of(self, created):
        h = (created - self.epoch) // HOUR
        return h if 0 <= h < self.T else None

    def hash_delta(self, h, hv):
        if h is None:
            self.h_out = (self.h_out + hv) % P61
        else:
            self.hb[h] = (int(self.hb[h]) + hv) % P61

    def hour_delta(self, created, status, total, sign):
        if self.owner is not None:
            self.owner.hour_delta(self, created, status, total, sign)
        h = (created - self.epoch) // HOUR
        if not 0 <= h < self.T:
            return  # outside the bucket axis: served from the mirror only
        v = np.zeros((2, NS), np.int64)
        v[0, status] = sign
        v[1, status] = sign * total
        self.hour_raw[h] += v
        self.hour.add(h, v)

    def prod_delta(self, created, items, sign):
        if self.owner is not None and items:
            self.owner.prod_delta(self, created, items, sign)
        d = (created - self.epoch) // DAY
        if not items or not 0 <= d < self.D:
            return
        v = np.zeros(self.P, np.int64)
        for pidx, sales in items:
            v[pidx] += sign * sales
        self.day.add(d, v)

    # ---------- edges (exact partial buckets from the mirror) ----------
    def edge_status(self, a, b):
        v = np.zeros((2, NS), np.int64)
        if b <= a:
            return v
        lo = np.searchsorted(self.b_created, a, "left")
        hi = np.searchsorted(self.b_created, b, "left")
        if hi > lo:
            st = self.b_status[lo:hi]
            v[0] = np.bincount(st, minlength=NS + 1)[:NS]
            v[1] = np.bincount(st, weights=self.b_total[lo:hi], minlength=NS + 1)[:NS].astype(np.int64)
        for o in self._ov_range(a, b):
            v[0, o[1]] += 1
            v[1, o[1]] += o[2]
        return v

    def edge_products(self, a, b):
        v = np.zeros(self.P, np.int64)
        if b <= a:
            return v
        lo = np.searchsorted(self.b_created, a, "left")
        hi = np.searchsorted(self.b_created, b, "left")
        ia, ib = self.b_ptr[lo], self.b_ptr[hi]
        if ib > ia:
            ost = np.repeat(self.b_status[lo:hi], np.diff(self.b_ptr[lo:hi + 1]))
            m = COUNTED[ost]
            v[:] = np.bincount(self.i_pidx[ia:ib][m], weights=self.i_sales[ia:ib][m],
                               minlength=self.P).astype(np.int64)
        for o in self._ov_range(a, b):
            if COUNTED[o[1]]:
                for pidx, sales in o[3].values():
                    v[pidx] += sales
        return v

    # ---------- range decomposition ----------
    def _split(self, f, t):
        """Split [f, t) into (outside-axis parts, axis part [f2, t2))."""
        lo_ax, hi_ax = self.epoch, self.epoch + self.T * HOUR
        f2, t2 = max(f, lo_ax), min(t, hi_ax)
        outside = [(f, min(t, lo_ax)), (max(f, hi_ax), t)]
        return outside, f2, t2

    def range_status(self, f, t):
        outside, f2, t2 = self._split(f, t)
        v = sum((self.edge_status(a, b) for a, b in outside), np.zeros((2, NS), np.int64))
        if t2 <= f2:
            return v
        h0 = _ceil_div(f2 - self.epoch, HOUR)
        h1 = (t2 - self.epoch) // HOUR
        if h0 >= h1:
            return v + self.edge_status(f2, t2)
        return (v + self.hour.range(h0, h1)
                + self.edge_status(f2, self.epoch + h0 * HOUR)
                + self.edge_status(self.epoch + h1 * HOUR, t2))

    def range_products(self, f, t):
        outside, f2, t2 = self._split(f, t)
        v = sum((self.edge_products(a, b) for a, b in outside), np.zeros(self.P, np.int64))
        if t2 <= f2:
            return v
        d0 = _ceil_div(f2 - self.epoch, DAY)
        d1 = (t2 - self.epoch) // DAY
        if d0 >= d1:
            return v + self.edge_products(f2, t2)
        return (v + self.day.range(d0, d1)
                + self.edge_products(f2, self.epoch + d0 * DAY)
                + self.edge_products(self.epoch + d1 * DAY, t2))

    def trend(self, f, t, step):
        """Buckets aligned to `step` (HOUR or DAY, UTC) intersected with [f, t) on the axis.
        Returns (bucket_starts_us, vectors[n, 2, NS])."""
        outside, f2, t2 = self._split(f, t)
        extra = {}  # orders moved outside the axis live only in the overflow mirror
        for a, b in outside:
            for o in self._ov_range(a, b):
                v = extra.setdefault(self.epoch + (o[0] - self.epoch) // step * step, np.zeros((2, NS), np.int64))
                v[0, o[1]] += 1
                v[1, o[1]] += o[2]
        starts, vals = self._axis_trend(f2, t2, step)
        if extra:
            keys = sorted(extra)
            starts = np.concatenate([starts, np.array(keys, np.int64)])
            vals = np.concatenate([vals, np.stack([extra[k] for k in keys])])
            order = np.argsort(starts, kind="stable")
            starts, vals = starts[order], vals[order]
        return starts, vals

    # ---------- cost (known before running the query) ----------
    def _rows(self, a, b):
        if b <= a:
            return 0, 0
        lo = np.searchsorted(self.b_created, a, "left")
        hi = np.searchsorted(self.b_created, b, "left")
        return int(hi - lo), int(self.b_ptr[hi] - self.b_ptr[lo])

    def cost(self, kind, f, t, step=HOUR):
        """Work units (~10 µs each): Fenwick node reads x node width + edge rows + output buckets."""
        _, f2, t2 = self._split(f, t)
        top = kind == "top"
        unit, width, n = (DAY, self.P, self.D) if top else (HOUR, 2 * NS, self.T)
        c = 1.0
        if t2 > f2:
            b0, b1 = _ceil_div(f2 - self.epoch, unit), (t2 - self.epoch) // unit
            edges = [(f2, self.epoch + b0 * unit), (self.epoch + b1 * unit, t2)] if b0 < b1 else [(f2, t2)]
            if b0 < b1 and kind != "trend":
                c += 2 * math.log2(n) * width / 1000
            for a, b in edges:
                o, i = self._rows(a, b)
                c += (i if top else o) / 200
            if kind == "trend":
                c += (t2 - f2) / step / 100
        return round(c, 2)

    # ---------- audit / repair ----------
    def outside_parts(self):
        return [(-(2 ** 62), self.epoch), (self.epoch + self.T * HOUR, 2 ** 62)]

    def audit_range(self, h0, h1):
        """[count, revenue, hash] for hour buckets [h0, h1), read from the Fenwick and bucket hashes."""
        v = self.hour.range(h0, h1)
        return [int(v[0].sum()), int(v[1].sum()), int(sum(self.hb[h0:h1].tolist())) % P61]

    def audit_outside(self):
        v = sum((self.edge_status(a, b) for a, b in self.outside_parts()), np.zeros((2, NS), np.int64))
        return [int(v[0].sum()), int(v[1].sum()), self.h_out]

    def mirror_rows(self, a, b):
        """(oid, status, total, created) of live mirror rows with created in [a, b)."""
        lo = np.searchsorted(self.b_created, a, "left")
        hi = np.searchsorted(self.b_created, b, "left")
        live = np.flatnonzero(self.b_status[lo:hi] != DELETED) + lo
        rows = list(zip(self.b_oid[live].tolist(), self.b_status[live].tolist(),
                        self.b_total[live].tolist(), self.b_created[live].tolist()))
        lo_k = bisect.bisect_left(self.ov_keys, (a,))
        hi_k = bisect.bisect_left(self.ov_keys, (b,))
        rows += [(oid, self.ov[oid][1], self.ov[oid][2], c) for c, oid in self.ov_keys[lo_k:hi_k]]
        return rows

    def rebuild_hour(self, h):
        """Surgical rebuild of one hour bucket (and its day's product vector) from the mirror."""
        s = self.epoch + h * HOUR
        new = self.edge_status(s, s + HOUR)
        delta = new - self.hour_raw[h]
        self.hour_raw[h] = new
        self.hour.add(h, delta)
        self.hb[h] = sum(row_hash(o, STATUSES[st], tot, c) for o, st, tot, c in self.mirror_rows(s, s + HOUR)) % P61
        d = h // 24
        ds = self.epoch + d * DAY
        pdelta = self.edge_products(ds, ds + DAY) - self.day.range(d, d + 1)
        self.day.add(d, pdelta)
        return {"hour_fixed": delta.tolist(), "day_products_fixed": int(np.abs(pdelta).sum())}

    def _axis_trend(self, f2, t2, step):
        if t2 <= f2:
            return np.zeros(0, np.int64), np.zeros((0, 2, NS), np.int64)
        m = step // HOUR
        b0 = (f2 - self.epoch) // step
        b1 = _ceil_div(t2 - self.epoch, step)
        vals = self.hour_raw[b0 * m:b1 * m].reshape(b1 - b0, m, 2, NS).sum(axis=1)
        starts = self.epoch + np.arange(b0, b1, dtype=np.int64) * step
        # partial buckets: full hours from the hourly tree, rows only for the partial hours
        if f2 > starts[0]:
            vals[0] = self.range_status(f2, min(int(starts[0]) + step, t2))
        last_end = int(starts[-1]) + step
        if t2 < last_end and (b1 - b0 > 1 or f2 == starts[0]):
            vals[-1] = self.range_status(max(int(starts[-1]), f2), t2)
        return starts, vals


class Engine:
    def __init__(self, headroom_days=60, lead_days=30):
        self.lock = threading.RLock()
        self.headroom_days, self.lead_days = headroom_days, lead_days
        self.tenants = {}
        self.tiers = {}
        self.names = {}
        self.categories = ["other"]  # product category vocabulary (code -> name)
        self._cat_index = {"other": 0}
        self.owner = None  # OwnerIndex across every store (zeta/owner.py), built after bootstrap
        self.stream_of = {}  # tenant id -> the store database stream it lives in
        self.epoch = 0
        self.T = 0
        self.recent = deque(maxlen=500_000)  # (xid, [(tid, hour|None, d_count, d_revenue, d_hash)])
        self._txlog = []
        self.stats = dict(bootstrapped_at=None, bootstrap_s=None, applied_lsn=0, last_xid=None,
                          last_commit_ts=None, last_apply_wall=None, lag_ms=None,
                          txns=0, changes=0, last_marker=None, errors=0, last_error=None)

    # ---------- bootstrap ----------
    @staticmethod
    def _copy(cur, sql, names, types=None):
        buf = io.BytesIO()
        cur.copy_expert(f"COPY ({sql}) TO STDOUT WITH (FORMAT csv)", buf)
        if buf.tell() == 0:
            return {n: np.zeros(0, np.int64) if (types or {}).get(n) is None else [] for n in names}
        buf.seek(0)
        ct = {n: (types or {}).get(n, pa.int64()) for n in names}
        tbl = pacsv.read_csv(buf, read_options=pacsv.ReadOptions(column_names=names),
                             convert_options=pacsv.ConvertOptions(column_types=ct))
        return {n: (tbl[n].to_numpy() if ct[n] == pa.int64() else tbl[n].to_pylist()) for n in names}

    def bootstrap(self, cur, stream=None):
        """Build the state of every store in the database `cur` reads (at its snapshot) and merge it in.
        Stores in other databases are untouched, so one store database can be rebuilt on its own."""
        t0 = time.time()
        status_case = "CASE status " + " ".join(f"WHEN '{s}' THEN {i}" for i, s in enumerate(STATUSES)) + " END"
        tenants = self._copy(cur, "SELECT tenant_id, tier, name FROM tenants", ["tid", "tier", "name"],
                             {"tier": pa.string(), "name": pa.string()})
        prods = self._copy(cur, "SELECT product_id, tenant_id, name, category FROM products ORDER BY tenant_id, product_id",
                           ["pid", "tid", "name", "cat"], {"name": pa.string(), "cat": pa.string()})
        o = self._copy(cur, f"SELECT order_id, tenant_id, {status_case}, total_cents, "
                            f"(extract(epoch FROM created_at) * 1000000)::bigint, {HASH_SQL} FROM orders",
                       ["oid", "tid", "st", "tot", "cr", "hx"])
        it = self._copy(cur, "SELECT order_id, line_no, product_id, qty::bigint * price_cents FROM order_items",
                        ["oid", "line", "pid", "sales"])
        t_read = time.time() - t0

        if len(o["cr"]):
            lo, hi = int(o["cr"].min()), int(o["cr"].max())
        else:
            lo = hi = int(time.time() * 1e6)
        epoch = (lo // DAY - self.lead_days) * DAY  # room for late / backdated orders
        D = (hi - epoch) // DAY + 1 + self.headroom_days
        T = D * 24

        # sort orders by (tenant, created, order_id); lay items out in that order (CSR)
        order = np.lexsort((o["oid"], o["cr"], o["tid"]))
        o = {k: v[order] for k, v in o.items()}
        by_id = np.argsort(o["oid"], kind="stable")
        k = np.searchsorted(o["oid"][by_id], it["oid"])
        ipos = by_id[k]
        iorder = np.lexsort((it["line"], ipos))
        ipos = ipos[iorder]
        it = {kk: v[iorder] for kk, v in it.items()}
        ptr = np.concatenate([[0], np.cumsum(np.bincount(ipos, minlength=len(o["oid"])))]).astype(np.int64)

        prod_tid = prods["tid"]
        tenants_new = {}
        for tid in sorted(set(tenants["tid"].tolist()) | set(o["tid"].tolist())):
            t = Tenant(tid, epoch, T)
            pa_, pb = np.searchsorted(prod_tid, tid, "left"), np.searchsorted(prod_tid, tid, "right")
            t.set_products(prods["pid"][pa_:pb], prods["name"][pa_:pb], [self.cat_code(c) for c in prods["cat"][pa_:pb]])
            lo_, hi_ = np.searchsorted(o["tid"], tid, "left"), np.searchsorted(o["tid"], tid, "right")
            ia, ib = ptr[lo_], ptr[hi_]
            t.b_created = o["cr"][lo_:hi_].copy()
            t.b_oid = o["oid"][lo_:hi_].copy()
            t.b_status = o["st"][lo_:hi_].astype(np.int8)
            t.b_total = o["tot"][lo_:hi_].copy()
            t.b_ptr = ptr[lo_:hi_ + 1] - ia
            t.i_line = it["line"][ia:ib].astype(np.int16)
            t.i_sales = it["sales"][ia:ib].copy()
            item_pids = it["pid"][ia:ib]
            if t.P:
                pidx = np.searchsorted(t.pid_arr, item_pids).clip(0, t.P - 1)
                bad = t.pid_arr[pidx] != item_pids
            else:
                pidx = np.zeros(len(item_pids), np.int64)
                bad = np.ones(len(item_pids), bool)
            for j in np.flatnonzero(bad):  # item references a product of another tenant / unknown
                pidx[j] = t.pidx_of(int(item_pids[j]))
            t.i_pidx = pidx.astype(np.int64)
            t.b_oid_pos = np.argsort(t.b_oid, kind="stable")
            t.b_oid_sorted = t.b_oid[t.b_oid_pos]

            # hourly [count|revenue] x status
            h = (t.b_created - epoch) // HOUR
            idx = h * NS + t.b_status
            t.hour_raw[:, 0, :] = np.bincount(idx, minlength=T * NS).reshape(T, NS)
            t.hour_raw[:, 1, :] = np.bincount(idx, weights=t.b_total, minlength=T * NS).astype(np.int64).reshape(T, NS)
            t.hour = Fenwick(t.hour_raw)
            # ledger hash per hour: exact modular sums via 30-bit halves (float64 bincount stays exact)
            hx = o["hx"][lo_:hi_]
            hs = np.bincount(h, weights=hx >> 30, minlength=T).astype(np.int64).tolist()
            ls = np.bincount(h, weights=hx & ((1 << 30) - 1), minlength=T).astype(np.int64).tolist()
            t.hb = np.array([((a << 30) + b) % P61 for a, b in zip(hs, ls)], np.int64)
            # daily product sales (counted statuses only)
            P = t.P
            counts = np.diff(t.b_ptr)
            d_item = np.repeat((t.b_created - epoch) // DAY, counts)
            m = COUNTED[np.repeat(t.b_status, counts)]
            raw = np.bincount(d_item[m] * P + t.i_pidx[m], weights=t.i_sales[m],
                              minlength=t.D * P).astype(np.int64).reshape(t.D, P) if P else np.zeros((t.D, 0), np.int64)
            t.day = Fenwick(raw)
            tenants_new[tid] = t

        self._install(tenants_new, dict(zip(tenants["tid"].tolist(), tenants["tier"])),
                      dict(zip(tenants["tid"].tolist(), tenants["name"])), stream)
        return {"orders": int(len(o["oid"])), "items": int(len(it["oid"])),
                "bootstrap_s": round(time.time() - t0, 2), "read_s": round(t_read, 2)}

    def _install(self, tenants_new, tiers, names, stream):
        """Merge freshly built (or restored) stores into the engine."""
        with self.lock:
            rebuilt = set(tenants_new)
            if self.owner is not None:  # stale until rebuilt: owner queries use the per-store path meanwhile
                self.owner.detach()
                self.owner = None
            self.tenants.update(tenants_new)
            if not self.T:
                t = next(iter(tenants_new.values()), None)
                if t is not None:
                    self.epoch, self.T = t.epoch, t.T
            self.tiers.update(tiers)
            self.names.update(names)
            if stream is not None:
                stream.tids = sorted(rebuilt)
                for tid in rebuilt:
                    self.stream_of[tid] = stream
            # audit log entries of rebuilt stores are now part of their base state
            self.recent = deque(((x, [e for e in es if e[0] not in rebuilt]) for x, es in self.recent),
                                maxlen=self.recent.maxlen)
            self.stats["bootstrapped_at"] = time.time()

    # ---------- checkpoints (zeta/snapshot.py writes and reads the files) ----------
    ARRAYS = ("hour_raw", "hb", "pid_arr", "pid_cat", "b_created", "b_oid", "b_status", "b_total", "b_ptr",
              "i_pidx", "i_sales", "i_line", "b_oid_sorted", "b_oid_pos")

    def capture(self, stream):
        """Copy everything needed to restore this store database's stores, consistent with
        stream.applied_lsn. Holds the engine lock only while copying (no I/O)."""
        with self.lock:
            out = {"lsn": stream.applied_lsn, "data_lsn": stream.data_lsn, "categories": list(self.categories),
                   "stores": {}}
            for tid in stream.tids:
                t = self.tenants[tid]
                arrays = {k: getattr(t, k).copy() for k in self.ARRAYS}
                arrays["hour_tree"] = t.hour.tree.copy()
                arrays["day_tree"] = t.day.tree.copy()
                out["stores"][tid] = {
                    "arrays": arrays,
                    "meta": {"epoch": t.epoch, "T": t.T, "h_out": t.h_out, "pid_names": list(t.pid_names),
                             "tier": self.tiers.get(tid), "name": self.names.get(tid),
                             "orders": int((t.b_status != DELETED).sum()) + len(t.ov),
                             # orders written after startup: [created_us, status, total, {line: [pidx, sales]}]
                             "ov": {str(oid): [o[0], o[1], o[2], {str(l): list(v) for l, v in o[3].items()}]
                                    for oid, o in t.ov.items()}}}
            return out

    def restore(self, snap, stream):
        """Install stores from a snapshot (the inverse of capture)."""
        t0 = time.time()
        codes = np.array([self.cat_code(c) for c in snap["categories"]], np.int16)  # codes can differ per run
        tenants_new, tiers, names, orders = {}, {}, {}, 0
        for tid, st in snap["stores"].items():
            a, m = st["arrays"], st["meta"]
            t = Tenant(tid, m["epoch"], m["T"])
            for k in self.ARRAYS:
                setattr(t, k, a[k])
            t.pid_cat = codes[a["pid_cat"]] if len(a["pid_cat"]) else a["pid_cat"]
            t.hour = Fenwick.from_tree(a["hour_tree"])
            t.day = Fenwick.from_tree(a["day_tree"])
            t.h_out = int(m["h_out"])
            t.pid_names = list(m["pid_names"])
            t.pid_index = {int(p): i for i, p in enumerate(t.pid_arr)}
            t.ov = {int(oid): [o[0], o[1], o[2], {int(l): tuple(v) for l, v in o[3].items()}] for oid, o in m["ov"].items()}
            t.ov_keys = sorted((o[0], oid) for oid, o in t.ov.items())
            tenants_new[tid], tiers[tid], names[tid] = t, m["tier"], m["name"]
            orders += m["orders"]
        self._install(tenants_new, tiers, names, stream)
        return {"orders": orders, "items": int(sum(len(t.i_sales) for t in tenants_new.values())),
                "bootstrap_s": round(time.time() - t0, 2), "restored": True}

    def cat_code(self, name):
        with self.lock:
            c = self._cat_index.get(name)
            if c is None:
                c = self._cat_index[name] = len(self.categories)
                self.categories.append(name)
            return c

    def build_owner(self):
        """(Re)build the all-store owner index from the per-store state; deltas keep it current afterwards."""
        from .owner import OwnerIndex
        with self.lock:
            for t in self.tenants.values():
                t.owner = None
            self.owner = OwnerIndex(self)
            return self.owner.stats

    # ---------- applying CDC transactions ----------
    def tenant(self, tid):
        t = self.tenants.get(tid)
        if t is None:
            t = self.tenants[tid] = Tenant(tid, self.epoch, self.T)
        return t

    def apply_txn(self, changes, lsn, xid, commit_us, stream=None):
        with self.lock:
            self._txlog = []
            for ch in changes:
                try:
                    self._apply(ch)
                except Exception as e:  # keep streaming; surface in /health
                    self.stats["errors"] += 1
                    self.stats["last_error"] = f"{type(e).__name__}: {e} in {ch}"
            if self._txlog:
                self.recent.append((xid, self._txlog))
            now = time.time()
            self.stats.update(applied_lsn=lsn, last_xid=xid, last_apply_wall=now,
                              txns=self.stats["txns"] + 1, changes=self.stats["changes"] + len(changes))
            if commit_us is not None:
                self.stats["last_commit_ts"] = commit_us / 1e6
                self.stats["lag_ms"] = round(now * 1000 - commit_us / 1000, 1)
            if stream is not None:
                stream.applied_lsn, stream.txns = lsn, stream.txns + 1
                if any(ch[0] != "MESSAGE" for ch in changes):  # real data, not just an audit/harness marker
                    stream.data_lsn, stream.last_apply_wall = lsn, now
                if commit_us is not None:
                    stream.last_commit_ts, stream.lag_ms = commit_us / 1e6, self.stats["lag_ms"]
                for ch in changes:
                    if ch[0] == "MESSAGE":
                        stream.last_marker = ch[3]
                        stream.markers.append(ch[3])

    def _apply(self, ch):
        kind, table, old, new = ch
        if kind == "MESSAGE":
            self.stats["last_marker"] = new
            return
        if table == "orders":
            self._apply_order(kind, old, new)
        elif table == "order_items":
            self._apply_item(kind, old, new)
        elif table == "products" and kind in ("INSERT", "UPDATE"):
            t = self.tenant(new["tenant_id"])
            i = t.pidx_of(new["product_id"], new["name"])
            t.pid_cat[i] = self.cat_code(new.get("category") or "other")
            if t.owner is not None:
                t.owner.p_cat[t.g_pmap[i]] = t.pid_cat[i]
        elif table == "tenants" and kind in ("INSERT", "UPDATE"):
            if new["tenant_id"] not in self.tenants and self.owner is not None:
                self.owner.detach()  # a brand-new store: owner queries fall back to per-store until rebuilt
                self.owner = None
            self.tenant(new["tenant_id"])
            self.tiers[new["tenant_id"]] = new["tier"]
        elif kind == "TRUNCATE":
            raise RuntimeError(f"TRUNCATE on {table}: full rebuild required")

    def _od(self, t, row, sign):
        """Order-level Z-set delta: hourly aggregates + ledger hash, logged with the txn for audits."""
        st, created, total = S_IDX[row["status"]], row["created_at"], row["total_cents"]
        t.hour_delta(created, st, total, sign)
        h = t.hour_of(created)
        hv = row_hash(row["order_id"], row["status"], total, created)
        hv = hv if sign > 0 else P61 - hv
        t.hash_delta(h, hv)
        self._txlog.append((t.tid, h, sign, sign * total, hv))

    def _apply_order(self, kind, old, new):
        if kind == "INSERT":
            t = self.tenant(new["tenant_id"])
            st = S_IDX[new["status"]]
            t.ov_add(new["order_id"], [new["created_at"], st, new["total_cents"], {}])
            self._od(t, new, +1)
            return
        t = self.tenant(old["tenant_id"])
        oid = old["order_id"]
        ov, pos = t.find(oid)
        if ov is None and pos is None:
            raise KeyError(f"order {oid} not in mirror")
        items = list(ov[3].values()) if ov is not None else t.base_items(pos)
        o_st = S_IDX[old["status"]]
        self._od(t, old, -1)
        if COUNTED[o_st]:
            t.prod_delta(old["created_at"], items, -1)
        if kind == "DELETE":
            if ov is not None:
                t.ov_remove(oid)
            else:
                t.set_base(pos, status=DELETED)
            return
        # UPDATE
        if new["tenant_id"] != old["tenant_id"]:
            raise NotImplementedError("tenant_id change")
        n_st = S_IDX[new["status"]]
        self._od(t, new, +1)
        if COUNTED[n_st]:
            t.prod_delta(new["created_at"], items, +1)
        if ov is None and new["created_at"] == int(t.b_created[pos]):
            t.set_base(pos, status=n_st, total=new["total_cents"])
            return
        if ov is None:
            ov = t.promote(pos)
        t.ov_remove(oid)
        ov[0], ov[1], ov[2] = new["created_at"], n_st, new["total_cents"]
        t.ov_add(oid, ov)

    def _apply_item(self, kind, old, new):
        if kind in ("DELETE", "UPDATE"):
            t = self.tenant(old["tenant_id"])
            o = self._order_for_items(t, old["order_id"])
            pidx, sales = o[3].pop(old["line_no"])
            if COUNTED[o[1]]:
                t.prod_delta(o[0], [(pidx, sales)], -1)
        if kind in ("INSERT", "UPDATE"):
            t = self.tenant(new["tenant_id"])
            o = self._order_for_items(t, new["order_id"])
            pidx = t.pidx_of(new["product_id"])
            sales = new["qty"] * new["price_cents"]
            o[3][new["line_no"]] = (pidx, sales)
            if COUNTED[o[1]]:
                t.prod_delta(o[0], [(pidx, sales)], +1)

    @staticmethod
    def _order_for_items(t, oid):
        ov, pos = t.find(oid)
        if ov is not None:
            return ov
        if pos is None:
            raise KeyError(f"order {oid} not in mirror")
        return t.promote(pos)

    # ---------- queries ----------
    def get(self, tid):
        t = self.tenants.get(tid)
        if t is None:
            raise KeyError(tid)
        return t

    def count(self, tid, f, t):
        with self.lock:
            return self.get(tid).range_status(f, t)

    def trend(self, tid, f, t, step):
        with self.lock:
            return self.get(tid).trend(f, t, step)

    def corrupt(self, tid, rng):
        """Demo only: silently damage one hour bucket, like a lost or double-applied delta."""
        with self.lock:
            t = self.get(tid)
            hours = np.flatnonzero(t.hour_raw[:, 0, :].sum(1))
            h = int(rng.choice(hours))
            v = np.zeros((2, NS), np.int64)
            v[0, 3], v[1, 3] = 1, 4999  # one phantom cancelled order
            t.hour_raw[h] += v
            t.hour.add(h, v)
            return h, t.epoch + h * HOUR

    @staticmethod
    def _topn(ten, v, n):
        pos = np.flatnonzero(v > 0)
        if len(pos) > n:
            kth = np.partition(v[pos], len(pos) - n)[len(pos) - n]
            pos = pos[v[pos] >= kth]
        order = np.lexsort((ten.pid_arr[pos], -v[pos]))[:n]
        return [(int(ten.pid_arr[i]), ten.pid_names[i], int(v[i])) for i in pos[order]]

    def top(self, tid, f, t, n):
        with self.lock:
            ten = self.get(tid)
            return self._topn(ten, ten.range_products(f, t), n)

    # ---------- owner: one company owns every store; aggregate across store databases ----------
    def owner_count(self, tids, f, t):
        """{tid: [2, NS] vector} for each store; the owner's totals are their sum (exact)."""
        with self.lock:
            if self.owner is not None:  # one pass over the all-store index, whatever the store count
                v = self.owner.range_status(f, t)
                pos = {tid: i for i, tid in enumerate(self.owner.tids)}
                return {tid: v[pos[tid]] for tid in tids if tid in pos}
            return {tid: self.tenants[tid].range_status(f, t) for tid in tids if tid in self.tenants}

    def owner_trend(self, tids, f, t, step):
        with self.lock:
            if self.owner is not None:
                starts, vals = self.owner.trend(f, t, step, self.owner.mask(tids))
                return starts, vals.sum(axis=1)
            parts = [self.tenants[tid].trend(f, t, step) for tid in tids if tid in self.tenants]
        parts = [(s, v) for s, v in parts if len(s)]
        if not parts:
            return np.zeros(0, np.int64), np.zeros((0, 2, NS), np.int64)
        # every store's buckets are UTC-aligned multiples of `step`, so they line up by index
        idx = [s // step for s, _ in parts]
        lo, hi = min(int(i[0]) for i in idx), max(int(i[-1]) for i in idx)
        acc = np.zeros((hi - lo + 1, 2, NS), np.int64)
        for i, (_, v) in zip(idx, parts):
            np.add.at(acc, i - lo, v)
        return np.arange(lo, hi + 1, dtype=np.int64) * step, acc

    def owner_top(self, tids, f, t, n):
        """Exact top-N across stores: the global top N is inside the union of each store's top N."""
        with self.lock:
            if self.owner is not None:
                return self.owner.top(f, t, n, self.owner.mask(tids))
            cands = []
            for tid in tids:
                ten = self.tenants.get(tid)
                if ten is not None:
                    cands += [(-s, pid, name, tid) for pid, name, s in self._topn(ten, ten.range_products(f, t), n)]
        cands.sort()
        return [(pid, name, -neg, tid) for neg, pid, name, tid in cands[:n]]

    def owner_sales_by_store(self, tids, f, t):
        """{tid: product sales (counted statuses)} for the selected stores, exact."""
        with self.lock:
            if self.owner is not None:
                o = self.owner
                v = o.range_products(f, t)
                per = np.bincount(o.p_store, weights=v, minlength=o.S).astype(np.int64)
                pos = {tid: i for i, tid in enumerate(o.tids)}
                return {tid: int(per[pos[tid]]) for tid in tids}
            return {tid: int(self.tenants[tid].range_products(f, t).sum()) for tid in tids}

    def owner_categories(self, tids, f, t):
        """{category: sales} across the selected stores (counted statuses), exact."""
        with self.lock:
            if self.owner is not None:
                v = self.owner.by_category(f, t, self.owner.mask(tids))
            else:
                v = np.zeros(len(self.categories), np.int64)
                for tid in tids:
                    ten = self.tenants[tid]
                    v += np.bincount(ten.pid_cat, weights=ten.range_products(f, t),
                                     minlength=len(self.categories)).astype(np.int64)
            return {self.categories[i]: int(x) for i, x in enumerate(v) if x}
