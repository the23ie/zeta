"""CDC: snapshot-exact bootstrap + logical replication stream (test_decoding) -> Z-set deltas.

Bootstrap without gaps or double counting:
  CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT  ->  load base tables inside that snapshot
  (SET TRANSACTION SNAPSHOT)  ->  stream from the slot's consistent point.
"""
import logging
import re
import select
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

log = logging.getLogger("zeta.cdc")

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

    def __init__(self, engine, base_dsn, db):
        self.engine, self.db = engine, db
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

    def bootstrap(self):
        """Create the slot with an exported snapshot and build this store's state from it."""
        admin = psycopg2.connect(self.dsn)
        admin.autocommit = True
        with admin.cursor() as c:
            c.execute("ALTER TABLE orders REPLICA IDENTITY FULL")
            c.execute("ALTER TABLE order_items REPLICA IDENTITY FULL")
            for _ in range(100):  # after a rebuild the old walsender may need a moment to exit
                c.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (self.slot,))
                row = c.fetchone()
                if row is None:
                    break
                if not row[0]:
                    c.execute("SELECT pg_drop_replication_slot(%s)", (self.slot,))
                    break
                time.sleep(0.1)
        admin.close()

        self.repl = psycopg2.connect(self.dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)
        self.rcur = self.repl.cursor()
        self.rcur.execute(f"CREATE_REPLICATION_SLOT {self.slot} LOGICAL test_decoding EXPORT_SNAPSHOT")
        _, consistent_point, snapshot, _ = self.rcur.fetchone()
        snap = psycopg2.connect(self.dsn)
        snap.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with snap.cursor() as c:
            c.execute("SET TRANSACTION SNAPSHOT %s", (snapshot,))
            self.boot = self.engine.bootstrap(c, self)
        snap.rollback()
        snap.close()
        hi, lo = consistent_point.split("/")
        self.applied_lsn = (int(hi, 16) << 32) | int(lo, 16)
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
                self.engine.apply_txn(self.changes, msg.data_start, self.xid, commit_us, self)
                self.changes, self.in_txn = [], False
                self.rcur.send_feedback(flush_lsn=msg.data_start)
            elif p.startswith("table "):
                self.changes.append(parse_change(p))
            elif p.startswith("message:"):
                m = MESSAGE.match(p)
                if m and m.group(2) == "zeta":
                    # skip-empty-xacts: a txn holding only a message has no BEGIN/COMMIT around it
                    if self.in_txn:
                        self.changes.append(("MESSAGE", None, None, m.group(3)))
                    else:
                        self.engine.apply_txn([("MESSAGE", None, None, m.group(3))], msg.data_start, None, None, self)

    def status(self):
        return {"db": self.db, "stores": self.tids, "error": self.error, "applied_lsn": lsn_str(self.applied_lsn),
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

    def __init__(self, engine, base_dsn, spec, workers=6):
        super().__init__(daemon=True, name="zeta-cdc")
        self.engine, self.base_dsn, self.spec, self.workers = engine, base_dsn, spec, workers
        self.streams = {}
        self.lock = threading.RLock()  # held while streams are read; rebuilds swap streams under it
        self.stop_evt = threading.Event()
        self.error = None

    def bootstrap(self):
        dbs = discover(self.base_dsn, self.spec)
        if not dbs:
            raise RuntimeError(f"no databases match {self.spec!r}")
        streams = [Stream(self.engine, self.base_dsn, db) for db in dbs]
        with ThreadPoolExecutor(self.workers) as ex:
            list(ex.map(lambda st: st.bootstrap(), streams))
        self.streams = {st.db: st for st in streams}

    def rebuild(self, db=None):
        """B-2: rebuild one store database (or all) from a fresh exported snapshot, others keep streaming."""
        targets = [db] if db else list(self.streams)
        for d in targets:
            with self.lock:
                old = self.streams.pop(d)
            old.close()
            new = Stream(self.engine, self.base_dsn, d)
            new.bootstrap()
            with self.lock:
                self.streams[d] = new
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
                                st.rcur.send_feedback(flush_lsn=st.applied_lsn, reply=True)
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

    def stop(self):
        self.stop_evt.set()
        self.join(timeout=5)
        for st in self.streams.values():
            st.close()
