"""Owner index: one company owns every store; these all-store structures make an owner question cost
the same whether there are 10 stores or 100 (instead of adding up every store's structures per query).

  * hourly Fenwick over node vectors [store x (count|revenue) x status]     -> totals, by-store, by-status, trend
  * daily Fenwick over every product of every store (sales, counted statuses) -> top products, by category
  * one time-sorted mirror of every store's rows (+ items, CSR)              -> exact partial-bucket edges
  * the overflow rows of all stores, time-sorted                             -> rows inserted/moved after bootstrap

It is built from the per-store state once (after bootstrap or a store rebuild) and then kept current by the
same Z-set deltas: every Tenant reports hour_delta / prod_delta / row changes here too.
A store filter (`stores=`) is just a mask over the store axis or over products' store.
"""
import bisect
import time

import numpy as np

from .engine import COUNTED, DAY, DELETED, HOUR, NS, Fenwick, _ceil_div


class OwnerIndex:
    def __init__(self, eng):
        t0 = time.time()
        self.eng = eng
        ts = [eng.tenants[tid] for tid in sorted(eng.tenants)]
        self.tids = [t.tid for t in ts]
        self.S = S = len(ts)
        self.epoch = min(t.epoch for t in ts)
        end = max(t.epoch + t.T * HOUR for t in ts)
        self.D = D = _ceil_div(end - self.epoch, DAY)
        self.T = T = D * 24

        # hourly [store, count|revenue, status]
        raw = np.zeros((T, S, 2, NS), np.int64)
        for s, t in enumerate(ts):
            off = (t.epoch - self.epoch) // HOUR
            raw[off:off + t.T, s] = t.hour_raw
            t.g_sidx = s
        self.hour_raw = raw
        self.hour_tot = raw.sum(axis=1)  # all stores together: an all-store trend is one slice
        self.hour = Fenwick(raw)

        # every product of every store; global product index = concatenation in store order
        self.p_pid = np.concatenate([t.pid_arr for t in ts]) if ts else np.zeros(0, np.int64)
        self.p_store = np.concatenate([np.full(t.P, s, np.int32) for s, t in enumerate(ts)])
        self.p_cat = np.concatenate([t.pid_cat for t in ts]).astype(np.int32)
        self.p_ref = [(t, i) for t in ts for i in range(t.P)]  # names live in the store
        draw = np.zeros((D, len(self.p_pid)), np.int64)
        off_p = 0
        for t in ts:
            t.g_pmap = list(range(off_p, off_p + t.P))
            doff = (t.epoch - self.epoch) // DAY
            if t.P:
                draw[doff:doff + t.D, off_p:off_p + t.P] = t.day.to_raw()
            off_p += t.P
        self.day = Fenwick(draw)
        del draw

        # one time-sorted mirror of every store's base rows (+ items as CSR in the same order)
        created = np.concatenate([t.b_created for t in ts])
        sidx = np.concatenate([np.full(len(t.b_created), s, np.int32) for s, t in enumerate(ts)])
        status = np.concatenate([t.b_status for t in ts])
        total = np.concatenate([t.b_total for t in ts])
        counts = np.concatenate([np.diff(t.b_ptr) for t in ts])
        item_g = np.concatenate([np.asarray(t.g_pmap, np.int64)[t.i_pidx] if len(t.i_pidx) else np.zeros(0, np.int64)
                                 for t in ts])
        item_sales = np.concatenate([t.i_sales for t in ts])
        order = np.argsort(created, kind="stable")
        self.g_created = created[order]
        self.g_sidx = sidx[order]
        self.g_status = status[order]
        self.g_total = total[order]
        ptr_c = np.concatenate([[0], np.cumsum(counts)])
        cnt_s = counts[order]
        self.g_ptr = np.concatenate([[0], np.cumsum(cnt_s)]).astype(np.int64)
        gather = np.repeat(ptr_c[:-1][order] - self.g_ptr[:-1], cnt_s) + np.arange(int(self.g_ptr[-1]))
        self.g_item = item_g[gather].astype(np.int64)
        self.g_item_sales = item_sales[gather]
        inv = np.empty(len(order), np.int64)
        inv[order] = np.arange(len(order))
        off = 0
        for t in ts:  # store row position -> global row, so a status change is mirrored in O(1)
            n = len(t.b_created)
            t.g_index = inv[off:off + n]
            off += n

        # overflow rows of every store, time-sorted
        self.ov = sorted((c, t.g_sidx, oid) for t in ts for c, oid in t.ov_keys)
        for t in ts:
            t.owner = self
        self.stats = {"stores": S, "rows": int(len(order)), "products": int(len(self.p_pid)),
                      "build_s": round(time.time() - t0, 2)}

    def detach(self):
        for tid in self.tids:
            t = self.eng.tenants.get(tid)
            if t is not None:
                t.owner = None

    # ---------- deltas (called by the stores) ----------
    def hour_delta(self, t, created, status, total, sign):
        h = (created - self.epoch) // HOUR
        if 0 <= h < self.T:
            s = t.g_sidx
            self.hour_raw[h, s, 0, status] += sign
            self.hour_raw[h, s, 1, status] += sign * total
            self.hour_tot[h, 0, status] += sign
            self.hour_tot[h, 1, status] += sign * total
            self.hour.add_point(h, (s, 0, status), sign)
            self.hour.add_point(h, (s, 1, status), sign * total)

    def prod_delta(self, t, created, items, sign):
        d = (created - self.epoch) // DAY
        if 0 <= d < self.D:
            for pidx, sales in items:
                self.day.add_point(d, (t.g_pmap[pidx],), sign * sales)

    def base_set(self, t, pos, status, total):
        g = t.g_index[pos]
        if status is not None:
            self.g_status[g] = status
        if total is not None:
            self.g_total[g] = total

    def ov_add(self, t, created, oid):
        bisect.insort(self.ov, (created, t.g_sidx, oid))

    def ov_remove(self, t, created, oid):
        k = bisect.bisect_left(self.ov, (created, t.g_sidx, oid))
        del self.ov[k]

    def add_product(self, t, pidx):
        t.g_pmap.append(len(self.p_pid))
        self.p_pid = np.append(self.p_pid, t.pid_arr[pidx])
        self.p_store = np.append(self.p_store, np.int32(t.g_sidx))
        self.p_cat = np.append(self.p_cat, np.int32(t.pid_cat[pidx]))
        self.p_ref.append((t, pidx))
        self.day.grow_cols(1)

    # ---------- edges ----------
    def _ov_rows(self, a, b):
        lo = bisect.bisect_left(self.ov, (a,))
        hi = bisect.bisect_left(self.ov, (b,))
        for c, s, oid in self.ov[lo:hi]:
            yield s, self.eng.tenants[self.tids[s]].ov[oid]

    def edge_status(self, a, b):
        """[store, count|revenue, status] for every row with created in [a, b)."""
        v = np.zeros((self.S, 2, NS), np.int64)
        if b <= a:
            return v
        lo = np.searchsorted(self.g_created, a, "left")
        hi = np.searchsorted(self.g_created, b, "left")
        if hi > lo:
            st = self.g_status[lo:hi]
            live = st != DELETED
            idx = self.g_sidx[lo:hi][live] * NS + st[live]
            v[:, 0, :] = np.bincount(idx, minlength=self.S * NS).reshape(self.S, NS)
            v[:, 1, :] = np.bincount(idx, weights=self.g_total[lo:hi][live],
                                     minlength=self.S * NS).astype(np.int64).reshape(self.S, NS)
        for s, o in self._ov_rows(a, b):
            v[s, 0, o[1]] += 1
            v[s, 1, o[1]] += o[2]
        return v

    def edge_products(self, a, b):
        P = len(self.p_pid)
        v = np.zeros(P, np.int64)
        if b <= a:
            return v
        lo = np.searchsorted(self.g_created, a, "left")
        hi = np.searchsorted(self.g_created, b, "left")
        ia, ib = self.g_ptr[lo], self.g_ptr[hi]
        if ib > ia:
            m = COUNTED[np.repeat(self.g_status[lo:hi], np.diff(self.g_ptr[lo:hi + 1]))]
            v[:] = np.bincount(self.g_item[ia:ib][m], weights=self.g_item_sales[ia:ib][m], minlength=P).astype(np.int64)
        for s, o in self._ov_rows(a, b):
            if COUNTED[o[1]]:
                t = self.eng.tenants[self.tids[s]]
                for pidx, sales in o[3].values():
                    v[t.g_pmap[pidx]] += sales
        return v

    # ---------- queries ----------
    def _split(self, f, t):
        lo_ax, hi_ax = self.epoch, self.epoch + self.T * HOUR
        return [(f, min(t, lo_ax)), (max(f, hi_ax), t)], max(f, lo_ax), min(t, hi_ax)

    def range_status(self, f, t):
        """[store, count|revenue, status] over [f, t), exact."""
        outside, f2, t2 = self._split(f, t)
        v = sum((self.edge_status(a, b) for a, b in outside), np.zeros((self.S, 2, NS), np.int64))
        if t2 <= f2:
            return v
        h0, h1 = _ceil_div(f2 - self.epoch, HOUR), (t2 - self.epoch) // HOUR
        if h0 >= h1:
            return v + self.edge_status(f2, t2)
        return (v + self.hour.range(h0, h1) + self.edge_status(f2, self.epoch + h0 * HOUR)
                + self.edge_status(self.epoch + h1 * HOUR, t2))

    def range_products(self, f, t):
        outside, f2, t2 = self._split(f, t)
        v = sum((self.edge_products(a, b) for a, b in outside), np.zeros(len(self.p_pid), np.int64))
        if t2 <= f2:
            return v
        d0, d1 = _ceil_div(f2 - self.epoch, DAY), (t2 - self.epoch) // DAY
        if d0 > d1:
            return v + self._day_part(f2, t2)
        mid = self.day.range(d0, d1) if d0 < d1 else 0
        return v + mid + self._day_part(f2, self.epoch + d0 * DAY) + self._day_part(self.epoch + d1 * DAY, t2)

    def _day_part(self, a, b):
        """[a, b) inside one day. Reads rows for at most half a day: a large part is the whole day
        (daily tree) minus the small leftover pieces."""
        if b <= a:
            return np.zeros(len(self.p_pid), np.int64)
        d = (a - self.epoch) // DAY
        ds = self.epoch + d * DAY
        if 2 * (b - a) > DAY and 0 <= d < self.D:
            return self.day.range(d, d + 1) - self.edge_products(ds, a) - self.edge_products(b, ds + DAY)
        return self.edge_products(a, b)

    def trend(self, f, t, step, sel):
        """UTC-aligned buckets of `step` over [f, t) for the selected stores (bool mask over the store axis).
        Returns (starts, [bucket, store, count|revenue, status]); for all stores the store axis has length 1."""
        allst = bool(sel.all())
        pick = (lambda v: v.sum(axis=0, keepdims=True)) if allst else (lambda v: v[sel])
        outside, f2, t2 = self._split(f, t)
        extra = {}
        for a, b in outside:
            for s, o in self._ov_rows(a, b):
                k = self.epoch + (o[0] - self.epoch) // step * step
                v = extra.setdefault(k, np.zeros((self.S, 2, NS), np.int64))
                v[s, 0, o[1]] += 1
                v[s, 1, o[1]] += o[2]
        k = 1 if allst else int(sel.sum())
        if t2 > f2:
            m = step // HOUR
            b0, b1 = (f2 - self.epoch) // step, _ceil_div(t2 - self.epoch, step)
            if allst:
                raw = self.hour_tot[b0 * m:b1 * m][:, None]
            else:
                raw = self.hour_raw[b0 * m:b1 * m][:, sel]  # copy only the selected stores
            vals = raw.reshape(b1 - b0, m, k, 2, NS).sum(axis=1)
            starts = self.epoch + np.arange(b0, b1, dtype=np.int64) * step
            # partial buckets: full hours from the hourly tree, rows only for the partial hours
            if f2 > starts[0]:
                vals[0] = pick(self.range_status(f2, min(int(starts[0]) + step, t2)))
            if t2 < int(starts[-1]) + step and (b1 - b0 > 1 or f2 == starts[0]):
                vals[-1] = pick(self.range_status(max(int(starts[-1]), f2), t2))
        else:
            starts, vals = np.zeros(0, np.int64), np.zeros((0, k, 2, NS), np.int64)
        if extra:
            keys = sorted(extra)
            starts = np.concatenate([starts, np.array(keys, np.int64)])
            vals = np.concatenate([vals, np.stack([pick(extra[x]) for x in keys])])
            o = np.argsort(starts, kind="stable")
            starts, vals = starts[o], vals[o]
        return starts, vals

    def top(self, f, t, n, sel):
        """Exact top-N products across the selected stores (ties by product id)."""
        v = self.range_products(f, t)
        v[~sel[self.p_store]] = 0
        pos = np.flatnonzero(v > 0)
        if len(pos) > n:
            kth = np.partition(v[pos], len(pos) - n)[len(pos) - n]
            pos = pos[v[pos] >= kth]
        pos = pos[np.lexsort((self.p_pid[pos], -v[pos]))[:n]]
        out = []
        for g in pos:
            t_, i = self.p_ref[g]
            out.append((int(self.p_pid[g]), t_.pid_names[i], int(v[g]), self.tids[self.p_store[g]]))
        return out

    def by_category(self, f, t, sel):
        """Sales per product category across the selected stores, exact."""
        v = self.range_products(f, t)
        v[~sel[self.p_store]] = 0
        return np.bincount(self.p_cat, weights=v, minlength=len(self.eng.categories)).astype(np.int64)

    def mask(self, tids):
        sel = np.zeros(self.S, bool)
        pos = {tid: s for s, tid in enumerate(self.tids)}
        for tid in tids:
            sel[pos[tid]] = True
        return sel
