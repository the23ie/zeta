"""CDC: snapshot-exact bootstrap + logical replication stream (test_decoding) -> Z-set deltas.

First start of a store database, without gaps or double counting:
  CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT  ->  load base tables inside that snapshot
  (SET TRANSACTION SNAPSHOT)  ->  stream from the slot's consistent point.

Restarts (checkpointing, zeta/snapshot.py):
  * the slot is kept; state is saved periodically with the LSN it reflects;
  * Postgres is only told "confirmed up to here" for LSNs that are saved on disk, so it keeps all WAL
    ZETA might still need;
  * on restart: load the snapshot, resume the slot, and drop any transaction at or below the snapshot's
    LSN (already counted). Anything that doesn't line up falls back to the full load above.
"""
import logging
import os
import re
import select
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

from . import snapshot

log = logging.getLogger("zeta.cdc")
HOLD_CONFIRM = os.environ.get("ZETA_TEST_HOLD_CONFIRM") == "1"

UNIX0 = datetime(1970, 1, 1, tzinfo=timezone.utc)
TOKEN = re.compile(r"(old-key:|new-tuple:)|([\w\"]+)\[([^\]]*)\]:('(?:[^']|'')*'|\S+)")
COMMIT = re.compile(r"COMMIT (\d+)(?: \(at (.+)\))?")
MESSAGE = re.compile(r"message: transactional: (\d) prefix: ([^,]*), sz: \d+ content:(.*)", re.S)


def ts_to_us(s):
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (dt - UNIX0) // _US


_US = datetime(1970, 1, 1, 0, 0, 0, 1, tzinfo=timezone.utc) - UNIX0


def lsn_str(lsn):
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


def lsn_int(s):
    hi, lo = s.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def _value(typ, raw):
    if raw == "null":
        return None
    if raw.startswith("'"):
        raw = raw[1:-1].replace("''", "'")
    if typ in ("bigint", "integer", "smallint"):
        return int(raw)
    if typ.startswith("timestamp"):
        return ts_to_us(raw)
    if typ == "numeric":
        return int(raw) if raw.lstrip("-").isdigit() else float(raw)
    return raw


def parse_change(line):
    """'table public.orders: UPDATE: old-key: a[int]:1 ... new-tuple: a[int]:2 ...'
    -> (kind, table, old, new)"""
    head, kind, rest = line.split(": ", 2) if line.count(": ") >= 2 else (*line.split(": ", 1), "")
    table = head.split(" ", 1)[1].split(".", 1)[-1].strip('"')
    kind = kind.rstrip(":")
    if kind == "TRUNCATE":
        return ("TRUNCATE", table, None, None)
    cur, old, new = {}, {}, {}
    target = new if kind == "INSERT" else old if kind == "DELETE" else new
    for m in TOKEN.finditer(rest):
        if m.group(1) == "old-key:":
            target = old
        elif m.group(1) == "new-tuple:":
            target = new
        else:
            target[m.group(2).strip('"')] = _value(m.group(3), m.group(4))
    if kind == "UPDATE" and not old:
        old = dict(new)  # no old row sent: identity unchanged (shouldn't happen with REPLICA IDENTITY FULL)
    return (kind, table, old or None, new or None)


class Stream:
    """One store database: its replication slot, snapshot bootstrap and per-store freshness."""

    def __init__(self, engine, base_dsn, db, snap_root=None):
        self.engine, self.db, self.snap_root = engine, db, snap_root
        self.dsn = f"{base_dsn} dbname={db}"
        self.slot = "zeta_" + re.sub(r"\W", "_", db)
        self.repl = self.rcur = None
        self.tids = []
        self.changes, self.xid, self.in_txn = [], None, False
        self.applied_lsn = 0
        self.last_commit_ts = self.last_apply_wall = self.lag_ms = None
        self.last_marker = None
        self.markers = deque(maxlen=256)  # recent markers: audits and harness flushes may interleave
        self.txns = 0
        self.last_fb = 0.0
        self.boot = {}
        self.error = None
        self.durable_lsn = 0     # newest LSN saved in a snapshot: the only position confirmed to Postgres
        self.skip_until = 0      # after a resume: transactions committed at or below this LSN are already counted
        self.snapshot_at = self.snapshot_s = None
        self.resumed = False
        self.skipped_txns = 0

    @property
    def confirm_lsn(self):
        if HOLD_CONFIRM:  # test only: never confirm, so a restart is resent changes already in the snapshot
            return 0
        return self.durable_lsn if self.snap_root else self.applied_lsn

    def _slot(self, c):
        """(active, confirmed_flush_lsn, wal_status) of this stream's slot, waiting briefly for an old sender to exit."""
        for _ in range(100):
            c.execute("SELECT active, confirmed_flush_lsn::text, wal_status FROM pg_replication_slots "
                      "WHERE slot_name = %s", (self.slot,))
            row = c.fetchone()
            if row is None or not row[0]:
                return row
            time.sleep(0.1)
        return row

    def start(self):
        """Resume from this database's snapshot if it lines up with the slot; otherwise do a full load."""
        snap = snapshot.load(self.snap_root, self.db) if self.snap_root else None
        if snap is not None:
            admin = psycopg2.connect(self.dsn)
            admin.autocommit = True
            with admin.cursor() as c:
                slot = self._slot(c)
            admin.close()
            if slot is None:
                why = "its replication slot is gone"
            elif slot[0]:
                why = "its replication slot is still in use"
            elif slot[2] == "lost":
                why = "Postgres removed WAL the slot needed"
            else:
                # The slot may be *past* the snapshot: ZETA only confirms saved LSNs, and psycopg2 moves the slot
                # further on idle keepalives only once every change it received was confirmed, i.e. when this
                # database had no unsaved changes. So nothing of this database lies between the two positions.
                why = None
            if why is None:
                t0 = time.time()
                self.boot = self.engine.restore(snap, self)
                self.applied_lsn = self.durable_lsn = self.skip_until = snap["lsn"]
                self.snapshot_at = snap["saved_at"]
                self.repl = psycopg2.connect(self.dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)
                self.rcur = self.repl.cursor()
                self.rcur.start_replication(slot_name=self.slot, decode=True, start_lsn=slot[1],
                                            options={"include-xids": "1", "include-timestamp": "1",
                                                     "skip-empty-xacts": "1"})
                self.resumed = True
                self.boot["bootstrap_s"] = round(time.time() - t0, 2)
                log.info("%s: resumed from snapshot at %s (slot at %s), %s orders, %.2fs", self.db,
                         lsn_str(snap["lsn"]), slot[1], self.boot.get("orders"), time.time() - t0)
                return
            log.warning("%s: not resuming from the snapshot (%s); full reload", self.db, why)
        self.bootstrap()

    def bootstrap(self):
        """Create the slot with an exported snapshot and build this store's state from it."""
        admin = psycopg2.connect(self.dsn)
        admin.autocommit = True
        with admin.cursor() as c:
            c.execute("ALTER TABLE orders REPLICA IDENTITY FULL")
            c.execute("ALTER TABLE order_items REPLICA IDENTITY FULL")
            row = self._slot(c)  # after a rebuild the old walsender may need a moment to exit
            if row is not None and not row[0]:
                c.execute("SELECT pg_drop_replication_slot(%s)", (self.slot,))
        admin.close()
        if self.snap_root:
            snapshot.drop(self.snap_root, self.db)  # it belongs to the old slot

        self.repl = psycopg2.connect(self.dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)
        self.rcur = self.repl.cursor()
        self.rcur.execute(f"CREATE_REPLICATION_SLOT {self.slot} LOGICAL test_decoding EXPORT_SNAPSHOT")
        _, consistent_point, snapshot_name, _ = self.rcur.fetchone()
        snap = psycopg2.connect(self.dsn)
        snap.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with snap.cursor() as c:
            c.execute("SET TRANSACTION SNAPSHOT %s", (snapshot_name,))
            self.boot = self.engine.bootstrap(c, self)
        snap.rollback()
        snap.close()
        # the source database holds everything up to the consistent point, so it is safe to confirm
        self.applied_lsn = self.durable_lsn = lsn_int(consistent_point)
        self.skip_until, self.resumed, self.snapshot_at = 0, False, None
        self.rcur.start_replication(slot_name=self.slot, decode=True, start_lsn=consistent_point,
                                    options={"include-xids": "1", "include-timestamp": "1",
                                             "skip-empty-xacts": "1"})
        log.info("%s: slot %s at %s, %s orders, stores %s", self.db, self.slot, consistent_point,
                 self.boot.get("orders"), self.tids)

    def close(self):
        try:
            self.repl.close()
        except Exception:
            pass

    def drain(self):
        """Apply every message waiting on this stream. Returns True if anything arrived."""
        got = False
        while True:
            msg = self.rcur.read_message()
            if msg is None:
                return got
            got = True
            p = msg.payload
            if p.startswith("BEGIN"):
                self.changes, self.in_txn = [], True
                self.xid = int(p.split()[1]) if len(p.split()) > 1 else None
            elif p.startswith("COMMIT"):
                m = COMMIT.match(p)
                commit_us = ts_to_us(m.group(2)) if m and m.group(2) else None
                if msg.data_start <= self.skip_until:  # already inside the snapshot we resumed from
                    self.skipped_txns += 1
                else:
                    self.engine.apply_txn(self.changes, msg.data_start, self.xid, commit_us, self)
                self.changes, self.in_txn = [], False
                self.rcur.send_feedback(flush_lsn=self.confirm_lsn)
            elif p.startswith("table "):
                self.changes.append(parse_change(p))
            elif p.startswith("message:"):
                m = MESSAGE.match(p)
                if m and m.group(2) == "zeta":
                    # skip-empty-xacts: a txn holding only a message has no BEGIN/COMMIT around it
                    if self.in_txn:
                        self.changes.append(("MESSAGE", None, None, m.group(3)))
                    elif msg.data_start > self.skip_until:
                        self.engine.apply_txn([("MESSAGE", None, None, m.group(3))], msg.data_start, None, None, self)

    def status(self):
        return {"db": self.db, "stores": self.tids, "error": self.error, "applied_lsn": lsn_str(self.applied_lsn),
                "saved_lsn": lsn_str(self.durable_lsn), "resumed_from_snapshot": self.resumed,
                "snapshot_age_s": round(time.time() - self.snapshot_at, 1) if self.snapshot_at else None,
                "snapshot_s": self.snapshot_s, "skipped_txns": self.skipped_txns,
                "lag_ms": self.lag_ms, "txns": self.txns, "last_marker": self.last_marker,
                "orders_at_bootstrap": self.boot.get("orders"), "bootstrap_s": self.boot.get("bootstrap_s")}


def discover(base_dsn, spec):
    """'t10m_*' -> every database with that prefix; 'a,b' -> exactly those databases."""
    if "*" not in spec:
        return [d for d in spec.split(",") if d]
    c = psycopg2.connect(f"{base_dsn} dbname=postgres")
    with c.cursor() as cur:
        cur.execute("SELECT datname FROM pg_database WHERE datname LIKE %s ORDER BY 1",
                    (spec.replace("_", "\\_").replace("*", "%"),))
        dbs = [r[0] for r in cur.fetchall()]
    c.close()
    return dbs


class Hub(threading.Thread):
    """Follows every store database's change stream from one thread (select over all slots)."""

    def __init__(self, engine, base_dsn, spec, workers=6, snap_root=None, snap_interval=60.0):
        super().__init__(daemon=True, name="zeta-cdc")
        self.engine, self.base_dsn, self.spec, self.workers = engine, base_dsn, spec, workers
        self.snap_root, self.snap_interval = snap_root, snap_interval
        self.snap_lock = threading.Lock()
        self.rebuild_lock = threading.Lock()
        self.streams = {}
        self.lock = threading.RLock()  # held while streams are read; rebuilds swap streams under it
        self.stop_evt = threading.Event()
        self.error = None

    def bootstrap(self):
        dbs = discover(self.base_dsn, self.spec)
        if not dbs:
            raise RuntimeError(f"no databases match {self.spec!r}")
        streams = [Stream(self.engine, self.base_dsn, db, self.snap_root) for db in dbs]
        with ThreadPoolExecutor(self.workers) as ex:
            list(ex.map(lambda st: st.start(), streams))
        self.streams = {st.db: st for st in streams}
        log.info("owner index: %s", self.engine.build_owner())

    def rebuild(self, db=None):
        """B-2: rebuild one store database (or all) from a fresh exported snapshot, others keep streaming.
        One rebuild at a time: a second request gets RuntimeError('busy') instead of racing the first."""
        if not self.rebuild_lock.acquire(blocking=False):
            raise RuntimeError("busy")
        try:
            return self._rebuild(db)
        finally:
            self.rebuild_lock.release()

    def _rebuild(self, db):
        targets = [db] if db else list(self.streams)
        for d in targets:
            with self.lock:
                old = self.streams.pop(d)
            old.close()
            new = Stream(self.engine, self.base_dsn, d, self.snap_root)
            new.bootstrap()  # always a full load from a fresh snapshot of the database
            with self.lock:
                self.streams[d] = new
        log.info("owner index: %s", self.engine.build_owner())
        return targets

    def stream_of(self, tid):
        return self.engine.stream_of.get(tid)

    def run(self):
        try:
            while not self.stop_evt.is_set():
                with self.lock:
                    streams = list(self.streams.values())
                    busy = False
                    now = time.time()
                    for st in streams:
                        if st.error:
                            continue
                        try:  # one broken store stream must not stop the others
                            busy |= st.drain()
                            if now - st.last_fb > 1.0:
                                st.rcur.send_feedback(flush_lsn=st.confirm_lsn, reply=True)
                                st.last_fb = now
                        except Exception as e:
                            st.error = f"{type(e).__name__}: {e}"
                            log.exception("stream %s stopped", st.db)
                    live = [st.rcur for st in streams if not st.error]
                if not busy:
                    try:
                        select.select(live, [], [], 0.05)
                    except (OSError, ValueError, psycopg2.Error):
                        time.sleep(0.05)  # a stream was swapped by a rebuild meanwhile
        except Exception as e:
            self.error = f"{type(e).__name__}: {e}"
            log.exception("CDC stopped")

    # ---------- checkpoints ----------
    def snapshot_all(self):
        """Save every store database that changed since its last snapshot; then confirm those LSNs."""
        if not self.snap_root:
            return {"saved": 0, "skipped": 0, "seconds": 0.0}
        t0, saved, skipped = time.time(), 0, 0
        with self.snap_lock:
            with self.lock:
                streams = list(self.streams.values())
            for st in streams:
                if st.error:
                    continue
                try:
                    lsn = snapshot.save(self.engine, st, self.snap_root)
                except Exception:
                    log.exception("snapshot of %s failed", st.db)
                    continue
                if lsn is None:
                    skipped += 1
                else:
                    st.durable_lsn, saved = lsn, saved + 1
        return {"saved": saved, "skipped": skipped, "seconds": round(time.time() - t0, 2)}

    def start_snapshots(self):
        if self.snap_root and self.snap_interval > 0:
            threading.Thread(target=self._snap_loop, daemon=True, name="zeta-snapshots").start()

    def _snap_loop(self):
        first = True
        while not self.stop_evt.is_set():
            if not first:
                self.stop_evt.wait(self.snap_interval)
            first = False
            if self.stop_evt.is_set():
                return
            r = self.snapshot_all()
            if r["saved"]:
                log.info("snapshots: %s", r)

    def stop(self):
        self.stop_evt.set()
        self.join(timeout=5)
        for st in self.streams.values():
            st.close()
