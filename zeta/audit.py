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


class Auditor:
    def __init__(self, engine, dsn, interval=60.0):
        self.engine, self.dsn, self.interval = engine, dsn, interval
        self.lock = threading.Lock()
        self.last = None
        self.history = []

    # ---------- helpers ----------
    def _wait_caught_up(self):
        token = "audit-" + uuid.uuid4().hex
        c = psycopg2.connect(self.dsn)
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("SELECT pg_logical_emit_message(true, 'zeta', %s)", (token,))
        c.close()
        t0 = time.time()
        while self.engine.stats["last_marker"] != token:
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
        with self.lock:
            return self._run(localize, repair)

    def _run(self, localize, repair):
        eng = self.engine
        t0 = time.time()
        conn = psycopg2.connect(self.dsn)
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
            self._wait_caught_up()

            xmax32, xip32 = xmax & 0xFFFFFFFF, {x & 0xFFFFFFFF for x in xip}
            res = {"at": time.time(), "snapshot": f"{xmin}:{xmax}:{','.join(map(str, xip))}",
                   "sql_ms": round(sql_ms, 1), "tenants": {}, "localized": []}
            with eng.lock:
                invisible = [e for xid, e in eng.recent if xid is None or xid >= xmax32 or xid in xip32]
                res["invisible_txns"] = len(invisible)
                for tid in sorted(set(db) | set(eng.tenants)):
                    t = eng.tenants.get(tid)
                    if t is None:
                        z = [0, 0, 0]
                    else:
                        a, o = t.audit_range(0, t.T), t.audit_outside()
                        z = self._minus([a[0] + o[0], a[1] + o[1], (a[2] + o[2]) % P61], self._adjust(invisible, tid))
                    d = db.get(tid, [0, 0, 0])
                    res["tenants"][tid] = {"ok": z == d, "db": d, "zeta": z}
                bad = [tid for tid, v in res["tenants"].items() if not v["ok"]]
                if localize:
                    for tid in bad:
                        if tid in eng.tenants:
                            res["localized"].append(self._localize(cur, eng.tenants[tid], invisible, repair))
            res["ok"] = all(v["ok"] for v in res["tenants"].values()) or (
                repair and bool(res["localized"]) and all(x.get("after_ok") for x in res["localized"]))
            res["took_ms"] = round((time.time() - t0) * 1000, 1)
        finally:
            conn.rollback()
            conn.close()
        self.last = res
        self.history = (self.history + [{k: res[k] for k in ("at", "ok", "took_ms", "sql_ms")}])[-50:]
        log.info("audit ok=%s tenants=%d took=%.0fms localized=%s", res["ok"], len(res["tenants"]),
                 res["took_ms"], res["localized"])
        return res

    def _localize(self, cur, t, invisible, repair):
        """Binary search over hour buckets, every comparison at the audit snapshot."""
        eng = self.engine
        n_sql = 0

        def db_range(h0, h1, agg):
            nonlocal n_sql
            n_sql += 1
            cur.execute(f"SELECT {agg} FROM orders WHERE tenant_id = %s AND created_at >= {US} AND created_at < {US}",
                        (t.tid, t.epoch + h0 * HOUR, t.epoch + h1 * HOUR))
            return [int(x) for x in cur.fetchone()]

        def z_range(h0, h1, fast):
            z = self._minus(t.audit_range(h0, h1), self._adjust(invisible, t.tid, h0, h1))
            return z[:2] + [0] if fast else z

        # outside the axis: those orders live only in the mirror
        cur.execute(f"SELECT {AGG} FROM orders WHERE tenant_id = %s AND (created_at < {US} OR created_at >= {US})",
                    (t.tid, t.epoch, t.epoch + t.T * HOUR))
        db_out = [int(x) for x in cur.fetchone()]
        z_out = self._minus(t.audit_outside(), self._adjust(invisible, t.tid, outside=True))
        if db_out != z_out:
            return {"tenant": t.tid, "where": "outside axis", "db": db_out, "zeta": z_out}

        fast = db_range(0, t.T, AGG_FAST) != z_range(0, t.T, True)  # count/revenue differ: skip md5 while searching
        agg = AGG_FAST if fast else AGG
        lo, hi = 0, t.T
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if db_range(lo, mid, agg) != z_range(lo, mid, fast):
                hi = mid
            else:
                lo = mid
        d, z = db_range(lo, lo + 1, AGG), z_range(lo, lo + 1, False)
        out = {"tenant": t.tid, "hour": lo, "hour_start": _iso(t.epoch + lo * HOUR), "sql_queries": n_sql + 1,
               "db": d, "zeta": z}
        if repair:
            out["repair"] = t.rebuild_hour(lo)
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
            except Exception:
                log.exception("audit failed")


def _iso(us):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(us / 1e6, timezone.utc).isoformat()
