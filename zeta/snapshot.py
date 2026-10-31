"""Checkpoints: save each store database's state with the WAL position (LSN) it reflects; load it on restart.

Layout, one folder per store database:
    snapshots/<db>/<lsn>/meta.json            LSN, categories, per-store metadata, orders written after startup
    snapshots/<db>/<lsn>/store_<tid>.npz      the store's numpy arrays (Fenwick trees, row copy, ...)
    snapshots/<db>/CURRENT                    name of the newest complete snapshot folder

A snapshot is written into a temporary folder, flushed to disk, renamed into place, and only then is CURRENT
switched (atomic rename). A crash at any point leaves the previous snapshot intact.
No pickle: arrays are plain .npz, everything else is JSON.
"""
import json
import logging
import os
import shutil
import time
import uuid

import numpy as np

log = logging.getLogger("zeta.snapshot")
VERSION = 1


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_file(path, write):
    with open(path, "wb") as f:
        write(f)
        f.flush()
        os.fsync(f.fileno())


def save(engine, stream, root):
    """Checkpoint one store database. Returns ("saved", lsn) after writing, or ("same", lsn) when no data changed
    since the last snapshot: then only markers/heartbeats arrived, the saved state still equals the current
    state, and the position can be confirmed without writing anything."""
    with engine.lock:  # read both positions together
        if stream.snapshot_at is not None and stream.data_lsn == stream.snap_data_lsn:
            return "same", stream.applied_lsn
    t0 = time.time()
    snap = engine.capture(stream)  # consistent copy under the engine lock, no I/O inside it
    copy_s = time.time() - t0
    base = os.path.join(root, stream.db)
    os.makedirs(base, exist_ok=True)
    tmp = os.path.join(base, f"tmp-{uuid.uuid4().hex}")
    os.makedirs(tmp)
    try:
        meta = {"version": VERSION, "db": stream.db, "lsn": snap["lsn"], "categories": snap["categories"],
                "saved_at": time.time(), "stores": {}}
        for tid, st in snap["stores"].items():
            _write_file(os.path.join(tmp, f"store_{tid}.npz"), lambda f, a=st["arrays"]: np.savez(f, **a))
            meta["stores"][str(tid)] = st["meta"]
        _write_file(os.path.join(tmp, "meta.json"), lambda f: f.write(json.dumps(meta).encode()))
        _fsync_dir(tmp)
        final = os.path.join(base, f"{snap['lsn']:016X}")
        if os.path.exists(final):
            shutil.rmtree(final)
        os.rename(tmp, final)
        cur_tmp = os.path.join(base, "CURRENT.tmp")
        _write_file(cur_tmp, lambda f: f.write(os.path.basename(final).encode()))
        os.replace(cur_tmp, os.path.join(base, "CURRENT"))
        _fsync_dir(base)
    except Exception:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    for name in os.listdir(base):  # keep only the current snapshot
        if name not in ("CURRENT", os.path.basename(final)):
            shutil.rmtree(os.path.join(base, name), ignore_errors=True)
    stream.snapshot_at = time.time()
    stream.snap_data_lsn = snap["data_lsn"]
    stream.snapshot_s = round(time.time() - t0, 2)
    log.debug("%s: snapshot at %X (copy %.2fs, total %.2fs)", stream.db, snap["lsn"], copy_s, stream.snapshot_s)
    return "saved", snap["lsn"]


def load(root, db):
    """The newest complete snapshot of a store database, or None (missing, unreadable or another version)."""
    base = os.path.join(root, db)
    try:
        with open(os.path.join(base, "CURRENT")) as f:
            folder = os.path.join(base, f.read().strip())
        with open(os.path.join(folder, "meta.json")) as f:
            meta = json.load(f)
        if meta.get("version") != VERSION or meta.get("db") != db:
            log.warning("%s: snapshot has version %s for %s; ignoring it", db, meta.get("version"), meta.get("db"))
            return None
        stores = {}
        for tid, m in meta["stores"].items():
            with np.load(os.path.join(folder, f"store_{tid}.npz"), allow_pickle=False) as z:
                stores[int(tid)] = {"arrays": {k: z[k] for k in z.files}, "meta": m}
        return {"lsn": int(meta["lsn"]), "categories": meta["categories"], "stores": stores,
                "saved_at": meta["saved_at"]}
    except FileNotFoundError:
        return None
    except Exception as e:  # a damaged snapshot must never stop ZETA: fall back to a full reload
        log.warning("%s: snapshot unreadable (%s); doing a full reload", db, e)
        return None


def drop(root, db):
    """Remove a store database's snapshots (after a full rebuild they no longer match the new slot)."""
    shutil.rmtree(os.path.join(root, db), ignore_errors=True)
