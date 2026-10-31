#!/usr/bin/env python3
"""Automated tenant-isolation test (FR-5) against a running ZETA server.

  python test_isolation.py                              # :8000 (stores 5 and 2)
  python test_isolation.py --api http://localhost:8001

Every route of the app is read from zeta.api and must belong to one class below; an unclassified
route fails the test, so a new endpoint cannot skip the isolation rules. For each route it checks:
  * no key / forged key / another store's digest -> 401 (public routes reveal nothing about stores)
  * store A's key on store B's URL, or asking for B through `stores=` -> 403
  * store A's key on cross-store routes -> 200 with only A's data (every tenant_id / database in the JSON is A's)
  * store A's key on operations -> 401
  * the owner key -> 200 and sees every store
Exit code 0 only if every check passes.
"""
import argparse
import logging
import sys

import httpx

from zeta.api import app
from zeta.guard import owner_key, tenant_key

logging.getLogger("httpx").setLevel(logging.WARNING)

PUBLIC = {"/dashboard", "/store", "/monitor", "/docs", "/docs/oauth2-redirect", "/redoc", "/openapi.json"}
HEALTH = "/health"  # public liveness only: {"ok": ...}
W = {"from": "2026-10-24T09:00:00Z", "to": "2026-10-31T09:00:00Z"}


def classify(path):
    if path in PUBLIC:
        return "public"
    if path == HEALTH:
        return "health"
    if path.startswith("/tenants/{tid}"):
        return "store"
    if path.startswith(("/owner/", "/analytics/")):
        return "cross"
    if path.startswith(("/admin/", "/markers/")) or path == "/monitor.json":
        return "ops"
    return None


def ids_in(obj, out):
    """Collect every tenant_id / database value anywhere in a JSON document."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in ("tenant_id", "database") and not isinstance(v, (dict, list)):
                out.add((k, v))
            ids_in(v, out)
    elif isinstance(obj, list):
        for v in obj:
            ids_in(v, out)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default="http://localhost:8000")
    ap.add_argument("--a", type=int, default=5, help="the store whose key is used")
    ap.add_argument("--b", type=int, default=2, help="another store")
    a = ap.parse_args()
    A, B = a.a, a.b
    api = httpx.Client(base_url=a.api, timeout=60)
    KA, KB, KO = tenant_key(A), tenant_key(B), owner_key()
    forged = [f"zt_{A}_" + "0" * 24, f"zt_{A}_" + KB.split("_")[2], "zo_" + "0" * 32, "za_" + "0" * 32]
    stores = {s["tenant_id"]: s["database"] for s in api.get("/owner/stores", headers={"X-API-Key": KO}).json()}
    a_db = stores[A]
    failures, checks = [], 0

    def expect(cond, what):
        nonlocal checks
        checks += 1
        if not cond:
            failures.append(what)

    def call(method, path, key=None, params=None):
        h = {"X-API-Key": key} if key else {}
        return api.request(method, path, params={**W, **(params or {})}, headers=h)

    routes = []
    for r in app.routes:
        for m in sorted(getattr(r, "methods", None) or []):
            if m in ("GET", "POST"):
                routes.append((m, r.path))
    for m, path in routes:
        cls = classify(path)
        expect(cls is not None, f"{m} {path}: route has no isolation class (add it to test_isolation.py)")
        if cls in (None, "public"):
            continue
        if cls == "health":
            for key in [None, KA, *forged]:
                body = call(m, path, key).json()
                expect(set(body) == {"ok"}, f"{m} {path} with key {str(key)[:6]}: exposes {sorted(body)[:6]}")
            expect(set(call(m, path, KO).json()) > {"ok"}, f"{m} {path}: owner key should get the detail")
            continue
        if m == "POST" and cls == "ops":  # never run audits / corrupt / rebuild with a valid key here
            pa = {"tenant": str(B)}
            for key in [None, KA, *forged]:
                c = call(m, path, key, pa).status_code
                expect(c == 401, f"{m} {path} with key {str(key)[:6]}: HTTP {c}, expected 401")
            continue
        url_a = path.replace("{tid}", str(A)).replace("{token}", "isolation-test")
        url_b = path.replace("{tid}", str(B)).replace("{token}", "isolation-test")
        for key in [None, *forged]:
            c = call(m, url_a, key).status_code
            expect(c == 401, f"{m} {url_a} with key {str(key)[:6]}: HTTP {c}, expected 401")
        if cls == "ops":
            c = call(m, url_a, KA).status_code
            expect(c == 401, f"{m} {url_a} with store {A}'s key: HTTP {c}, expected 401")
            c = call(m, url_a, KO).status_code
            expect(c == 200, f"{m} {url_a} with the owner key: HTTP {c}, expected 200")
            continue
        if cls == "store":
            c = call(m, url_b, KA).status_code
            expect(c == 403, f"{m} {url_b} with store {A}'s key: HTTP {c}, expected 403")
            r = call(m, url_a, KA)
            expect(r.status_code == 200, f"{m} {url_a} with its own key: HTTP {r.status_code}")
            leaked = {v for k, v in ids_in(r.json(), set()) if v not in (A, a_db)}
            expect(not leaked, f"{m} {url_a}: response mentions other stores {leaked}")
            continue
        # cross-store routes
        for sp in (str(B), f"{A},{B}"):
            c = call(m, url_a, KA, {"stores": sp}).status_code
            expect(c == 403, f"{m} {url_a}?stores={sp} with store {A}'s key: HTTP {c}, expected 403")
        r = call(m, url_a, KA)
        expect(r.status_code == 200, f"{m} {url_a} with store {A}'s key: HTTP {r.status_code}")
        if r.status_code == 200:
            body = r.json()
            leaked = {v for k, v in ids_in(body, set()) if v not in (A, a_db)}
            expect(not leaked, f"{m} {url_a} with store {A}'s key: response mentions other stores {sorted(map(str, leaked))[:5]}")
            if isinstance(body, dict) and "stores" in body:
                expect(body["stores"] == 1, f"{m} {url_a}: store key aggregated {body['stores']} stores")
        ro = call(m, url_a, KO)
        expect(ro.status_code == 200, f"{m} {url_a} with the owner key: HTTP {ro.status_code}")

    # the same number through both doors
    own = call("GET", f"/tenants/{A}/orders/count", KA).json()["count"]
    via = call("GET", "/owner/orders/count", KA).json()["count"]
    expect(own == via, f"store {A}: /tenants count {own} != /owner count {via} with its own key")
    expect(len(api.get("/owner/stores", headers={"X-API-Key": KO}).json()) == len(stores), "owner should see every store")

    n_routes = sum(1 for _, p in routes if classify(p) not in (None, "public"))
    print(f"isolation test against {a.api}: {len(routes)} routes ({n_routes} protected), {checks} checks, "
          f"{len(failures)} failures")
    for f in failures:
        print("  FAIL", f)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
