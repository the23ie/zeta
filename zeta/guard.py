"""Tenant isolation (FR-5) and cost-aware per-tenant rate limits (B-3).

Keys: an API key is an HMAC bound to exactly one tenant; the tenant in the URL must match
the key. The owner and admin keys can read every tenant and use operations.
A store's key can be rotated: each store has a key version (in .zeta_key_versions.json); bumping it
issues a new key and the old one stops working immediately, with nothing else stored.

Limits: each tenant has a token bucket sized by tier. A request is charged its *cost*,
which ZETA knows before running it (Fenwick nodes + edge rows from its own index),
not a flat 1 per request: a 90-day top-N with wide edges costs more than a 1-hour count.

  python -m zeta.guard 1 2 3            # print keys for stores 1, 2, 3 plus the owner and admin keys
  python -m zeta.guard --all t10m       # print every store's key (stores found in databases t10m_*)
  python -m zeta.guard --rotate 7       # issue a new key for store 7; the old one stops working
"""
import json
import hmac
import math
import os
import sys
import threading
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SECRET_FILE = os.environ.get("ZETA_SECRET_FILE", os.path.join(_ROOT, ".zeta_secret"))
_VERSIONS_FILE = os.environ.get("ZETA_KEY_VERSIONS", os.path.join(_ROOT, ".zeta_key_versions.json"))
# ZETA_SECRET, else the project's .zeta_secret file, else a dev default (anyone with the code can mint keys)
SECRET = (os.environ.get("ZETA_SECRET")
          or (open(_SECRET_FILE).read().strip() if os.path.exists(_SECRET_FILE) else "zeta-dev-secret")).encode()
AUTH = os.environ.get("ZETA_AUTH", "1") == "1"
LIMITS = os.environ.get("ZETA_LIMITS", "1") == "1"
TIER_CAPACITY = {"enterprise": 20000, "growth": 8000, "starter": 4000, "owner": 200000}  # cost units; refill = capacity / 2 per s
TIER_CONCURRENCY = {"enterprise": 8, "growth": 4, "starter": 2, "owner": 16}  # queries one store may run at the same time


def _mac(msg, n):
    return hmac.new(SECRET, msg.encode(), "sha256").hexdigest()[:n]


_versions = {"mtime": None, "v": {}}
_vlock = threading.RLock()  # re-entrant: rotate_tenant_key reads the versions while holding it


def _key_versions():
    """Per-store key versions, re-read when the file changes (so a CLI rotation reaches a running server)."""
    try:
        mtime = os.stat(_VERSIONS_FILE).st_mtime_ns
    except FileNotFoundError:
        return {}
    with _vlock:
        if mtime != _versions["mtime"]:
            with open(_VERSIONS_FILE) as f:
                _versions["v"] = {int(k): int(v) for k, v in json.load(f).items()}
            _versions["mtime"] = mtime
        return _versions["v"]


def tenant_key(tid):
    v = _key_versions().get(int(tid), 0)
    label = f"tenant:{tid}" if v == 0 else f"tenant:{tid}:v{v}"  # version 0 keeps the original keys valid
    return f"zt_{tid}_{_mac(label, 24)}"


def rotate_tenant_key(tid):
    """Issue a new key for one store; its previous key stops working at once."""
    with _vlock:
        versions = dict(_key_versions())
        versions[int(tid)] = versions.get(int(tid), 0) + 1
        tmp = _VERSIONS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump({str(k): v for k, v in sorted(versions.items())}, f, indent=1)
        os.replace(tmp, _VERSIONS_FILE)
    return tenant_key(tid)


def admin_key():
    return f"za_{_mac('admin', 32)}"


def owner_key():
    """The company that owns every store: reads any store and the cross-store /owner routes."""
    return f"zo_{_mac('owner', 32)}"


def check_key(key, tid=None):
    """Returns 'admin', the tenant id the key is bound to, or None."""
    if not key:
        return None
    if hmac.compare_digest(key, admin_key()):
        return "admin"
    if hmac.compare_digest(key, owner_key()):
        return "owner"
    parts = key.split("_")
    if len(parts) == 3 and parts[0] == "zt" and parts[1].isdigit():
        k_tid = int(parts[1])
        if hmac.compare_digest(key, tenant_key(k_tid)):
            return k_tid
    return None


class Limiter:
    def __init__(self):
        self.lock = threading.Lock()
        self.buckets = {}  # tid -> [tokens, last_refill, capacity]
        self.rejected = {}
        self.inflight = {}  # tid -> queries running now

    def enter(self, tid, tier):
        """Start a query for this store unless it already runs its tier's maximum at once (B-3: a store firing many
        heavy queries in parallel gets 429 instead of taking every worker thread). Pair with leave()."""
        cap = TIER_CONCURRENCY.get(tier, TIER_CONCURRENCY["starter"])
        with self.lock:
            n = self.inflight.get(tid, 0)
            if n >= cap:
                self.rejected[tid] = self.rejected.get(tid, 0) + 1
                return False
            self.inflight[tid] = n + 1
            return True

    def leave(self, tid):
        with self.lock:
            self.inflight[tid] = max(0, self.inflight.get(tid, 0) - 1)

    def charge(self, tid, tier, cost):
        """Returns (allowed, tokens_left, retry_after_s)."""
        cap = TIER_CAPACITY.get(tier, TIER_CAPACITY["starter"])
        rate = cap / 2
        with self.lock:
            b = self.buckets.get(tid)
            now = time.monotonic()
            if b is None:
                b = self.buckets[tid] = [cap, now, cap]
            b[0] = min(cap, b[0] + (now - b[1]) * rate)
            b[1] = now
            if cost > cap:
                cost = cap  # a single request can always eventually run
            if b[0] >= cost:
                b[0] -= cost
                return True, b[0], 0.0
            self.rejected[tid] = self.rejected.get(tid, 0) + 1
            return False, b[0], math.ceil((cost - b[0]) / rate * 1000) / 1000

    def snapshot(self):
        with self.lock:
            return {tid: {"tokens": round(b[0]), "capacity": b[2], "rejected": self.rejected.get(tid, 0),
                          "in_flight": self.inflight.get(tid, 0)} for tid, b in self.buckets.items()}


def _stores_in(prefix):
    import psycopg2
    from zeta.cdc import discover
    pg = os.environ.get("ZETA_PG", "host=localhost port=5433 user=zeta")
    tids = []
    for db in discover(pg, prefix + "_*"):
        c = psycopg2.connect(f"{pg} dbname={db}")
        with c.cursor() as cur:
            cur.execute("SELECT tenant_id FROM tenants ORDER BY 1")
            tids += [r[0] for r in cur.fetchall()]
        c.close()
    return sorted(tids)


if __name__ == "__main__":
    args = sys.argv[1:]
    if args[:1] == ["--rotate"]:
        for a in args[1:]:
            print(f"tenant {a}: {rotate_tenant_key(int(a))}   (new key; the previous one no longer works)")
        sys.exit(0)
    if args[:1] == ["--all"]:
        args = [str(t) for t in _stores_in(args[1] if len(args) > 1 else "t10m")]
    for a in args:
        print(f"tenant {a}: {tenant_key(int(a))}")
    print(f"owner:    {owner_key()}")
    print(f"admin:    {admin_key()}")
