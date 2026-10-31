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


class CDC(threading.Thread):
    def __init__(self, engine, dsn, slot="zeta_slot"):
        super().__init__(daemon=True, name="zeta-cdc")
        self.engine, self.dsn, self.slot = engine, dsn, slot
        self.stop_evt = threading.Event()
        self.ready = threading.Event()
        self.error = None
        self.server_wal_end = 0

    def bootstrap(self):
        """Create the slot with an exported snapshot and build engine state from it."""
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
        log.info("slot %s consistent_point=%s snapshot=%s", self.slot, consistent_point, snapshot)

        snap = psycopg2.connect(self.dsn)
        snap.set_session(isolation_level="REPEATABLE READ", readonly=True)
        with snap.cursor() as c:
            c.execute("SET TRANSACTION SNAPSHOT %s", (snapshot,))
            self.engine.bootstrap(c)
        snap.rollback()
        snap.close()
        hi, lo = consistent_point.split("/")
        self.engine.stats["applied_lsn"] = (int(hi, 16) << 32) | int(lo, 16)
        self.rcur.start_replication(slot_name=self.slot, decode=True, start_lsn=consistent_point,
                                    options={"include-xids": "1", "include-timestamp": "1",
                                             "skip-empty-xacts": "1"})

    def stop(self):
        self.stop_evt.set()
        self.join(timeout=5)
        self.repl.close()

    def run(self):
        try:
            self._loop()
        except Exception as e:
            self.error = f"{type(e).__name__}: {e}"
            log.exception("CDC stopped")

    def _loop(self):
        changes, xid, in_txn = [], None, False
        last_fb = 0.0
        cur = self.rcur
        self.ready.set()
        while not self.stop_evt.is_set():
            msg = cur.read_message()
            if msg is None:
                now = time.time()
                if now - last_fb > 1.0:
                    cur.send_feedback(flush_lsn=self.engine.stats["applied_lsn"], reply=True)
                    last_fb = now
                select.select([cur], [], [], 0.2)
                self.server_wal_end = getattr(cur, "wal_end", 0) or self.server_wal_end
                continue
            p = msg.payload
            if p.startswith("BEGIN"):
                changes, xid, in_txn = [], int(p.split()[1]) if len(p.split()) > 1 else None, True
            elif p.startswith("COMMIT"):
                m = COMMIT.match(p)
                commit_us = ts_to_us(m.group(2)) if m and m.group(2) else None
                self.engine.apply_txn(changes, msg.data_start, xid, commit_us)
                changes, in_txn = [], False
                cur.send_feedback(flush_lsn=msg.data_start)
            elif p.startswith("table "):
                changes.append(parse_change(p))
            elif p.startswith("message:"):
                m = MESSAGE.match(p)
                if m and m.group(2) == "zeta":
                    # skip-empty-xacts: a txn holding only a message has no BEGIN/COMMIT around it
                    if in_txn:
                        changes.append(("MESSAGE", None, None, m.group(3)))
                    else:
                        self.engine.apply_txn([("MESSAGE", None, None, m.group(3))], msg.data_start, None, None)
