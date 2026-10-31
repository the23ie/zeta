"""Tenant isolation (FR-5) and cost-aware per-tenant rate limits (B-3).

Keys: an API key is an HMAC bound to exactly one tenant; the tenant in the URL must match
the key. The admin key can read every tenant and use /admin and /monitor.

Limits: each tenant has a token bucket sized by tier. A request is charged its *cost*,
which ZETA knows before running it (Fenwick nodes + edge rows from its own index),
not a flat 1 per request: a 90-day top-N with wide edges costs more than a 1-hour count.

  python -m zeta.guard 1 2 3     # print keys for tenants 1, 2, 3 and the admin key
"""
import hmac
import math
import os
import sys
import threading
import time

_SECRET_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".zeta_secret")
# ZETA_SECRET, else the project's .zeta_secret file, else a dev default (anyone with the code can mint keys)
SECRET = (os.environ.get("ZETA_SECRET")
          or (open(_SECRET_FILE).read().strip() if os.path.exists(_SECRET_FILE) else "zeta-dev-secret")).encode()
AUTH = os.environ.get("ZETA_AUTH", "1") == "1"
LIMITS = os.environ.get("ZETA_LIMITS", "1") == "1"
TIER_CAPACITY = {"enterprise": 20000, "growth": 8000, "starter": 4000, "owner": 200000}  # cost units; refill = capacity / 2 per s


def _mac(msg, n):
    return hmac.new(SECRET, msg.encode(), "sha256").hexdigest()[:n]


def tenant_key(tid):
    return f"zt_{tid}_{_mac(f'tenant:{tid}', 24)}"


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
            return {tid: {"tokens": round(b[0]), "capacity": b[2], "rejected": self.rejected.get(tid, 0)}
                    for tid, b in self.buckets.items()}


if __name__ == "__main__":
    for a in sys.argv[1:]:
        print(f"tenant {a}: {tenant_key(int(a))}")
    print(f"owner:    {owner_key()}")
    print(f"admin:    {admin_key()}")
