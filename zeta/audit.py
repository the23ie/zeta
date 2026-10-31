"""Snapshot-exact audit of ZETA against the database (Correctness, B-2, B-5).

1. Open a REPEATABLE READ transaction and read pg_current_snapshot() (xmin:xmax:xip).
2. In that same snapshot, compute per tenant: count, sum(total_cents), ledger hash.
3. Emit a WAL marker and wait until ZETA has applied it: ZETA now contains every
   transaction visible in the snapshot, and possibly a few newer ones.
4. ZETA's value at the snapshot = current value - deltas of transactions the snapshot
   cannot see (every delta is logged with its xid). Subtraction does the work.
5. On mismatch, binary-search the hour buckets (same snapshot) to find the bad bucket,
   and optionally rebuild only that bucket from the mirror.
"""
import logging
import threading
import time
import uuid

import psycopg2

from .engine import HASH_SQL, HOUR, P61

log = logging.getLogger("zeta.audit")

AGG = f"count(*), coalesce(sum(total_cents), 0), mod(coalesce(sum({HASH_SQL}::numeric), 0), {P61})"
AGG_FAST = "count(*), coalesce(sum(total_cents), 0), 0"
US = "('epoch'::timestamptz + %s * interval '1 microsecond')"


class Busy(Exception):
    """An audit is already running (the API answers 409 instead of queueing another one)."""


class Auditor:
    def __init__(self, engine, hub, interval=60.0):
        self.engine, self.hub, self.interval = engine, hub, interval
        self.lock = threading.Lock()
        self.last = None
        self.history = []

    # ---------- helpers ----------
    @staticmethod
    def _wait_caught_up(stream):
        """Write a marker into this store database's WAL; wait until its stream has applied it."""
        token = "audit-" + uuid.uuid4().hex
        c = psycopg2.connect(stream.dsn)
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("SELECT pg_logical_emit_message(true, 'zeta', %s)", (token,))
        c.close()
        t0 = time.time()
        while token not in stream.markers:
            if time.time() - t0 > 30:
                raise TimeoutError("ZETA did not catch up within 30s")
            time.sleep(0.002)

    @staticmethod
    def _adjust(invisible, tid, h0=None, h1=None, outside=False):
        """Sum of deltas for this tenant (and hour range) from transactions the snapshot cannot see."""
        c = r = hh = 0
        for entries in invisible:
            for t_, h, dc, dr, dh in entries:
                if t_ != tid:
                    continue
                if outside and h is not None:
                    continue
                if h0 is not None and (h is None or not h0 <= h < h1):
                    continue
                c, r, hh = c + dc, r + dr, hh + dh
        return c, r, hh

    @staticmethod
    def _minus(z, a):
        return [z[0] - a[0], z[1] - a[1], (z[2] - a[2]) % P61]

    # ---------- audit ----------
    def run(self, localize=False, repair=False):
        if not self.lock.acquire(blocking=False):
            raise Busy("an audit is already running")
        try:
            return self._run(localize, repair)
        finally:
            self.lock.release()

    def _run(self, localize, repair):
        t0 = time.time()
        with self.hub.lock:
            streams = list(self.hub.streams.values())
        res = {"at": time.time(), "databases": len(streams), "tenants": {}, "localized": [],
               "sql_ms": 0.0, "invisible_txns": 0, "snapshots": {}}
        for st in streams:  # one snapshot-exact comparison per store database
            r = self._audit_db(st, localize, repair)
            res["tenants"].update(r["tenants"])
            res["localized"] += r["localized"]
            res["sql_ms"] += r["sql_ms"]
            res["invisible_txns"] += r["invisible_txns"]
            res["snapshots"][st.db] = r["snapshot"]
        res["sql_ms"] = round(res["sql_ms"], 1)
        res["snapshot"] = f"one per database ({len(streams)})"
        res["ok"] = all(v["ok"] for v in res["tenants"].values())  # after any repair
        res["took_ms"] = round((time.time() - t0) * 1000, 1)
        self.last = res
        self.history = (self.history + [{k: res[k] for k in ("at", "ok", "took_ms", "sql_ms")}])[-50:]
        log.info("audit ok=%s databases=%d stores=%d took=%.0fms localized=%s", res["ok"], len(streams),
                 len(res["tenants"]), res["took_ms"], res["localized"])
        return res

    def _audit_db(self, st, localize, repair):
        eng = self.engine
        conn = psycopg2.connect(st.dsn)
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
        cur = conn.cursor()
        try:
            cur.execute("SELECT pg_snapshot_xmin(s)::text::bigint, pg_snapshot_xmax(s)::text::bigint, "
                        "array(SELECT pg_snapshot_xip(s)::text::bigint) FROM (SELECT pg_current_snapshot() s) x")
            xmin, xmax, xip = cur.fetchone()
            ts = time.time()
            cur.execute(f"SELECT tenant_id, {AGG} FROM orders GROUP BY 1")
            db = {tid: [int(c), int(r), int(h)] for tid, c, r, h in cur.fetchall()}
            sql_ms = (time.time() - ts) * 1000
            self._wait_caught_up(st)

            # xids are cluster-wide, so the snapshot of this database also classifies every logged delta
            xmax32, xip32 = xmax & 0xFFFFFFFF, {x & 0xFFFFFFFF for x in xip}
            out = {"snapshot": f"{xmin}:{xmax}:{','.join(map(str, xip))}", "sql_ms": sql_ms,
                   "tenants": {}, "localized": []}
            vis = (xmax32, xip32)
            with eng.lock:
                out["invisible_txns"] = len(self._invisible(vis))
                for tid in sorted(set(db) | set(st.tids)):
                    z = self._z_total(tid, vis) if tid in eng.tenants else [0, 0, 0]
                    d = db.get(tid, [0, 0, 0])
                    out["tenants"][tid] = {"ok": z == d, "db": d, "zeta": z, "database": st.db}
            # The search runs its SQL *outside* the engine lock (change processing and queries keep going);
            # every ZETA-side value is re-read under the lock with the deltas the snapshot cannot see removed.
            if localize:
                for tid, v in out["tenants"].items():
                    if v["ok"] or tid not in eng.tenants:
                        continue
                    for _ in range(50):  # repair keeps going until the store matches again
                        loc = self._localize(cur, tid, vis, repair)
                        out["localized"].append(loc)
                        if not repair or "hour" not in loc or not loc.get("after_ok"):
                            break
                        with eng.lock:
                            v["zeta"] = self._z_total(tid, vis)
                        if v["zeta"] == v["db"]:
                            v["ok"] = v["repaired"] = True
                            break
            return out
        finally:
            conn.rollback()
            conn.close()

    def _invisible(self, vis):
        """Logged deltas of transactions the audit snapshot cannot see (call under the engine lock)."""
        xmax32, xip32 = vis
        return [e for xid, e in self.engine.recent if xid is None or xid >= xmax32 or xid in xip32]

    def _z_total(self, tid, vis):
        """ZETA's [count, revenue, hash] for a whole store at the audit snapshot (call under the engine lock)."""
        t = self.engine.tenants[tid]
        a, o = t.audit_range(0, t.T), t.audit_outside()
        return self._minus([a[0] + o[0], a[1] + o[1], (a[2] + o[2]) % P61], self._adjust(self._invisible(vis), tid))

    def _localize(self, cur, tid, vis, repair):
        """Binary search over hour buckets for one bad hour, every comparison at the audit snapshot."""
        eng = self.engine
        n_sql = 0
        with eng.lock:
            t = eng.tenants[tid]
            epoch, T = t.epoch, t.T

        def db_range(h0, h1, agg):
            nonlocal n_sql
            n_sql += 1
            cur.execute(f"SELECT {agg} FROM orders WHERE tenant_id = %s AND created_at >= {US} AND created_at < {US}",
                        (tid, epoch + h0 * HOUR, epoch + h1 * HOUR))
            return [int(x) for x in cur.fetchone()]

        def z_range(h0, h1, fast):
            with eng.lock:
                z = self._minus(eng.tenants[tid].audit_range(h0, h1), self._adjust(self._invisible(vis), tid, h0, h1))
            return z[:2] + [0] if fast else z

        # outside the axis: those orders live only in the mirror
        cur.execute(f"SELECT {AGG} FROM orders WHERE tenant_id = %s AND (created_at < {US} OR created_at >= {US})",
                    (tid, epoch, epoch + T * HOUR))
        db_out = [int(x) for x in cur.fetchone()]
        with eng.lock:
            z_out = self._minus(eng.tenants[tid].audit_outside(), self._adjust(self._invisible(vis), tid, outside=True))
        if db_out != z_out:
            return {"tenant": tid, "where": "outside axis", "db": db_out, "zeta": z_out}

        fast = db_range(0, T, AGG_FAST) != z_range(0, T, True)  # count/revenue differ: skip md5 while searching
        agg = AGG_FAST if fast else AGG
        lo, hi = 0, T
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if db_range(lo, mid, agg) != z_range(lo, mid, fast):
                hi = mid
            else:
                lo = mid
        d, z = db_range(lo, lo + 1, AGG), z_range(lo, lo + 1, False)
        out = {"tenant": tid, "hour": lo, "hour_start": _iso(epoch + lo * HOUR), "sql_queries": n_sql + 1,
               "db": d, "zeta": z}
        if repair:
            with eng.lock:
                out["repair"] = eng.tenants[tid].rebuild_hour(lo)
            out["after"] = z2 = z_range(lo, lo + 1, False)
            out["after_ok"] = z2 == d
        return out

    # ---------- background loop ----------
    def start(self):
        if self.interval > 0:
            threading.Thread(target=self._loop, daemon=True, name="zeta-audit").start()

    def _loop(self):
        while True:
            time.sleep(self.interval)
            try:
                self.run()
            except Busy:
                pass  # one was started from the dashboard; skip this round
            except Exception:
                log.exception("audit failed")


def _iso(us):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(us / 1e6, timezone.utc).isoformat()
