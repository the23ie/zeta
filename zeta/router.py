"""ZETA router: one front door for several ZETA nodes, each following a share of the store databases.

  * A store's request (/tenants/{id}/..., or any route called with a store key) goes to the node holding that store.
  * An owner's request goes to every node at once (only the nodes holding the requested stores when `stores=` is
    given) and the answers are merged exactly: counts and sums are added, trends merged bucket by bucket, top-N lists
    re-ranked (the global top N is inside the union of every node's top N), and ratios (average order value, cancel
    and return rates) recomputed from their added numerators and denominators (zeta/defs.py), never averaged.
  * Keys are checked by the nodes (same .zeta_secret); the router only reads a store key's id to route it.

  ZETA_NODES=http://localhost:8011,http://localhost:8012 uvicorn zeta.router:app --port 8001
"""
import asyncio
import logging
import os
import re
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from . import guard
from .defs import GROUPS, METRICS, RATIOS, STATUSES, WEEKDAYS, cancel_flag, change, kpis, ratio, status_lists

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("zeta.router")
logging.getLogger("httpx").setLevel(logging.WARNING)

NODES = [u.strip().rstrip("/") for u in os.environ.get("ZETA_NODES", "http://localhost:8011,http://localhost:8012").split(",")
         if u.strip()]
ADMIN = {"X-API-Key": guard.admin_key()}
HERE = Path(__file__).parent
owner_of = {}  # store id -> node url
client = None
LAT = defaultdict(lambda: deque(maxlen=5000))
STARTED = time.time()


async def refresh_routes():
    table = {}
    for n in NODES:
        r = await client.get(n + "/owner/stores", headers=ADMIN)
        r.raise_for_status()
        for s in r.json():
            table[s["tenant_id"]] = n
    owner_of.clear()
    owner_of.update(table)


async def _keep_routes_fresh():
    while True:
        await asyncio.sleep(30)
        try:
            await refresh_routes()
        except Exception as e:
            log.warning("route refresh failed: %s", e)


@asynccontextmanager
async def lifespan(app):
    global client
    client = httpx.AsyncClient(timeout=600)
    for attempt in range(600):  # nodes may still be starting
        try:
            await refresh_routes()
            break
        except Exception:
            await asyncio.sleep(1)
    log.info("router: %d nodes, %d stores: %s", len(NODES), len(owner_of),
             {n: sum(1 for v in owner_of.values() if v == n) for n in NODES})
    task = asyncio.create_task(_keep_routes_fresh())
    yield
    task.cancel()
    await client.aclose()


app = FastAPI(title="ZETA router", lifespan=lifespan)


@app.middleware("http")
async def timing(request: Request, call_next):
    t0 = time.perf_counter()
    resp = await call_next(request)
    route = request.scope.get("route")
    if route is not None and route.path.startswith(("/tenants", "/owner", "/analytics")):
        LAT[route.path].append(((time.perf_counter() - t0) * 1000, resp.status_code))
    return resp


# ---------------- plumbing ----------------
def _key(req):
    return req.headers.get("x-api-key") or req.query_params.get("key")


def _store_of_key(req):
    m = re.match(r"zt_(\d+)_", _key(req) or "")
    return int(m.group(1)) if m else None


def _relay(r):
    keep = {k: v for k, v in r.headers.items() if k.lower() in ("x-zeta-cost", "x-ratelimit-remaining", "retry-after")}
    return Response(content=r.content, status_code=r.status_code, media_type=r.headers.get("content-type"), headers=keep)


async def _node_for(tid):
    if tid not in owner_of:
        try:
            await refresh_routes()
        except Exception:
            pass
    return owner_of.get(tid)


async def _call(node, method, path, req, params):
    h = {"X-API-Key": _key(req)} if _key(req) else {}
    return await client.request(method, node + path, params=params, headers=h)


async def _targets(req, path):
    """[(node, params)]. A store key -> only its node. `stores=` -> only the nodes holding those stores."""
    params = dict(req.query_params)
    tid = _store_of_key(req)
    if tid is not None:
        return [(await _node_for(tid) or NODES[0], params)]
    if params.get("stores"):
        try:
            ids = sorted({int(x) for x in params["stores"].split(",") if x.strip()})
        except ValueError:
            return [(NODES[0], params)]  # let a node answer 400
        by_node = defaultdict(list)
        for i in ids:
            node = await _node_for(i)
            if node is None:
                return [(NODES[0], params)]  # the node answers 404 for unknown stores
            by_node[node].append(i)
        return [(n, {**params, "stores": ",".join(map(str, v))}) for n, v in by_node.items()]
    return [(n, params) for n in NODES]


async def _fan(req, path, method="GET", targets=None):
    targets = targets or await _targets(req, path)
    try:
        rs = await asyncio.gather(*[_call(n, method, path, req, p) for n, p in targets])
    except httpx.HTTPError as e:
        return None, JSONResponse({"detail": f"a ZETA node is unavailable: {e}"}, status_code=503)
    bad = next((r for r in rs if r.status_code != 200), None)
    if bad is not None:
        return None, _relay(bad)
    if len(rs) == 1:
        return None, _relay(rs[0])  # one node holds everything asked for: its answer is already complete
    return [r.json() for r in rs], None


def _fresh(bodies):
    fs = [b.get("freshness") or {} for b in bodies]
    lags = [f["max_cdc_lag_ms"] for f in fs if f.get("max_cdc_lag_ms") is not None]
    ago = [f["updated_s_ago"] for f in fs if f.get("updated_s_ago") is not None]
    return {"databases": sum(f.get("databases") or 0 for f in fs), "max_cdc_lag_ms": max(lags) if lags else None,
            "updated_s_ago": min(ago) if ago else None, "nodes": len(bodies)}


def _env(bodies, t0, **body):
    b0 = bodies[0]
    return {"owner": True, "stores": sum(b.get("stores", 0) for b in bodies), "from": b0.get("from"), "to": b0.get("to"),
            **body, "freshness": _fresh(bodies), "took_ms": round((time.perf_counter() - t0) * 1000, 3),
            "nodes": len(bodies)}


def _sum_status(dicts):
    out = {s: {"count": 0, "revenue_cents": 0} for s in STATUSES}
    for d in dicts:
        for s in STATUSES:
            out[s]["count"] += d[s]["count"]
            out[s]["revenue_cents"] += d[s]["revenue_cents"]
    return out


# ---------------- pages ----------------
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return (HERE / "dashboard.html").read_text()


@app.get("/store", response_class=HTMLResponse)
def store_page():
    return (HERE / "store.html").read_text()


@app.get("/monitor")
def monitor_page():
    return RedirectResponse("/dashboard#ops")


# ---------------- store routes: forwarded to the node holding the store ----------------
@app.get("/tenants/{tid}/{rest:path}")
async def store_route(tid: int, rest: str, request: Request):
    node = await _node_for(tid)
    if node is None:
        node = NODES[0]  # the node answers 401/403/404 exactly as a single ZETA would
    try:
        return _relay(await _call(node, "GET", f"/tenants/{tid}/{rest}", request, dict(request.query_params)))
    except httpx.HTTPError as e:
        return JSONResponse({"detail": f"the node holding store {tid} is unavailable: {e}"}, status_code=503)


# ---------------- owner routes: fan out, merge exactly ----------------
@app.get("/owner/stores")
async def owner_stores(request: Request):
    bodies, resp = await _fan(request, "/owner/stores")
    return resp or sorted((s for b in bodies for s in b), key=lambda s: s["tenant_id"])


@app.get("/owner/orders/count")
async def owner_count(request: Request):
    t0 = time.perf_counter()
    bodies, resp = await _fan(request, "/owner/orders/count")
    return resp or _env(bodies, t0, status=bodies[0].get("status"), count=sum(b["count"] for b in bodies),
                        revenue_cents=sum(b["revenue_cents"] for b in bodies))


@app.get("/owner/orders/by-store")
async def owner_by_store(request: Request):
    t0 = time.perf_counter()
    bodies, resp = await _fan(request, "/owner/orders/by-store")
    if resp:
        return resp
    rows = sorted((r for b in bodies for r in b["by_store"]), key=lambda r: (-r["revenue_cents"], r["tenant_id"]))
    return _env(bodies, t0, status=bodies[0].get("status"), by_store=rows)


@app.get("/owner/orders/by-status")
async def owner_by_status(request: Request):
    t0 = time.perf_counter()
    bodies, resp = await _fan(request, "/owner/orders/by-status")
    return resp or _env(bodies, t0, by_status=_sum_status([b["by_status"] for b in bodies]))


def _merge_buckets(bodies):
    acc = {}
    for b in bodies:
        for x in b["buckets"]:
            c = acc.setdefault(x["bucket"], [0, 0])
            c[0] += x["count"]
            c[1] += x["revenue_cents"]
    return [{"bucket": k, "count": v[0], "revenue_cents": v[1]} for k, v in sorted(acc.items())]


@app.get("/owner/orders/trend")
async def owner_trend(request: Request):
    t0 = time.perf_counter()
    bodies, resp = await _fan(request, "/owner/orders/trend")
    return resp or _env(bodies, t0, granularity=bodies[0].get("granularity"), status=bodies[0].get("status"),
                        buckets=_merge_buckets(bodies))


@app.get("/owner/products/top")
async def owner_top(request: Request):
    t0 = time.perf_counter()
    bodies, resp = await _fan(request, "/owner/products/top")
    if resp:
        return resp
    n = int(request.query_params.get("n", 10))
    rows = sorted((p for b in bodies for p in b["products"]), key=lambda p: (-p["sales_cents"], p["product_id"]))[:n]
    return _env(bodies, t0, n=n, products=rows)


# ---------------- analytics ----------------
@app.get("/analytics/summary")
async def analytics_summary(request: Request):
    t0 = time.perf_counter()
    bodies, resp = await _fan(request, "/analytics/summary")
    if resp:
        return resp
    cur = _sum_status([b["by_status"] for b in bodies])
    prev = _sum_status([b["previous_by_status"] for b in bodies])
    k, kp = kpis(*status_lists(cur)), kpis(*status_lists(prev))
    cats = defaultdict(int)
    for b in bodies:
        for c, v in b["categories"].items():
            cats[c] += v
    return _env(bodies, t0, kpis=k, previous=kp, change_pct=change(k, kp), previous_period=bodies[0]["previous_period"],
                by_status=cur, previous_by_status=prev, categories=dict(cats))


@app.get("/analytics/stores")
async def analytics_stores(request: Request):
    t0 = time.perf_counter()
    bodies, resp = await _fan(request, "/analytics/stores")
    if resp:
        return resp
    rows = [r for b in bodies for r in b["stores_data"]]
    n = sum(r["orders"] for r in rows)
    rev = sum(r["revenue"] for r in rows)
    canc, ret = sum(r["cancelled_orders"] for r in rows), sum(r["returned_orders"] for r in rows)
    overall = {"orders": n, "revenue": rev, "net_sales": sum(r["net_sales"] for r in rows),
               "aov": ratio("aov", rev, n), "cancel_rate": ratio("cancel_rate", canc, n),
               "return_rate": ratio("return_rate", ret, n)}
    for r in rows:  # the "high cancel rate" flag compares with the overall rate across *all* nodes
        r["flags"] = [f for f in r["flags"] if not f.startswith("cancel rate")]
        f = cancel_flag(r, overall)
        if f:
            r["flags"].append(f)
    rows.sort(key=lambda r: (-r["revenue"], r["tenant_id"]))
    return _env(bodies, t0, overall=overall, stores_data=rows)


@app.get("/analytics/heatmap")
async def analytics_heatmap(request: Request):
    t0 = time.perf_counter()
    bodies, resp = await _fan(request, "/analytics/heatmap")
    if resp:
        return resp
    grid = [[sum(b["grid"][d][h] for b in bodies) for h in range(24)] for d in range(7)]
    return _env(bodies, t0, weekdays=WEEKDAYS, grid=grid, timezone="UTC")


def _sum_rows(bodies):
    acc, extra = {}, {}
    for b in bodies:
        for r in b["rows"]:
            acc[r["key"]] = acc.get(r["key"], 0) + r["value"]
            extra.setdefault(r["key"], {k: v for k, v in r.items() if k not in ("key", "value")})
    return acc, extra


def _order_keys(by, keys):
    if by == "weekday":
        return [d for d in WEEKDAYS if d in keys]
    return sorted(keys)  # days (ISO dates) and "HH:00 UTC" sort correctly as text


@app.get("/analytics/explore")
async def analytics_explore(request: Request):
    t0 = time.perf_counter()
    q = dict(request.query_params)
    metric, by = q.get("metric", "orders"), q.get("by", "store")
    n = int(q.get("n", 20))
    if _store_of_key(request) is not None or metric not in METRICS or by not in GROUPS:
        bodies, resp = await _fan(request, "/analytics/explore")  # one node, or let a node answer 400
        return resp
    targets = await _targets(request, "/analytics/explore")
    if len(targets) == 1:
        return _relay(await _call(targets[0][0], "GET", "/analytics/explore", request, targets[0][1]))
    if metric in RATIOS and by in ("none", "day", "hour_of_day", "weekday"):
        # a ratio is recomputed from its added numerator and denominator, never averaged across nodes
        num, den = RATIOS[metric]
        parts = {}
        for m in (num, den):
            tg = [(node, {**p, "metric": m}) for node, p in targets]
            bodies, resp = await _fan(request, "/analytics/explore", targets=tg)
            if resp:
                return resp
            parts[m] = _sum_rows(bodies)[0]
        keys = _order_keys(by, set(parts[num]) | set(parts[den]))
        rows = [{"key": k, "value": ratio(metric, parts[num].get(k, 0), parts[den].get(k, 0))} for k in keys]
        return _env(bodies, t0, metric=metric, by=by, definition=METRICS[metric], rows=rows)
    tg = [(node, {**p, "n": "200"} if by == "category" else p) for node, p in targets]
    bodies, resp = await _fan(request, "/analytics/explore", targets=tg)
    if resp:
        return resp
    if by in ("store", "product"):  # disjoint rows per node: concatenate and re-rank
        rows = sorted((r for b in bodies for r in b["rows"]), key=lambda r: (-r["value"], r["key"]))
        if by == "product":
            rows = sorted(rows, key=lambda r: (-r["value"], r.get("product_id", 0)))
        rows = rows[:n]
    else:
        acc, extra = _sum_rows(bodies)
        if by in ("category", "status", "none"):
            keys = sorted(acc, key=lambda k: (-acc[k], k)) if by == "category" else list(acc)
            if by == "status":
                keys = [s for s in STATUSES if s in acc]
        else:
            keys = _order_keys(by, acc)
        rows = [{"key": k, "value": acc[k], **extra[k]} for k in keys]
        if by == "category":
            rows = rows[:n]
    return _env(bodies, t0, metric=metric, by=by, definition=METRICS[metric], rows=rows)


# ---------------- health / operations ----------------
def _operator(req):
    return not guard.AUTH or guard.check_key(_key(req)) in ("admin", "owner")


async def _node_health():
    out = []
    for n in NODES:
        try:
            r = await client.get(n + "/health", headers=ADMIN)
            out.append((n, r.json() if r.status_code == 200 else None))
        except httpx.HTTPError:
            out.append((n, None))
    return out


def _merge_health(hs):
    ok = [h for _, h in hs if h]
    lags = [h["max_cdc_lag_ms"] for h in ok if h.get("max_cdc_lag_ms") is not None]
    snaps = [h.get("snapshots") or {} for h in ok]
    return {"ok": len(ok) == len(hs) and all(h["ok"] for h in ok), "nodes": [{"url": n, "up": h is not None,
            "databases": h["databases"] if h else None} for n, h in hs],
            "cdc_error": next((h["cdc_error"] for h in ok if h.get("cdc_error")), None),
            "broken_streams": [s for h in ok for s in h["broken_streams"]],
            "databases": sum(h["databases"] for h in ok), "tenants": sum(h["tenants"] for h in ok),
            "orders": sum(h["orders"] for h in ok), "items": sum(h["items"] for h in ok),
            "bootstrap_s": max((h["bootstrap_s"] or 0 for h in ok), default=None),
            "max_cdc_lag_ms": max(lags) if lags else None,
            "audit_ok": all(h["audit_ok"] for h in ok) if ok and all(h["audit_ok"] is not None for h in ok) else None,
            "snapshots": {"enabled": all(s.get("enabled") for s in snaps),
                          "resumed_databases": sum(s.get("resumed_databases") or 0 for s in snaps)},
            "txns": sum(h["txns"] for h in ok), "errors": sum(h["errors"] for h in ok),
            "markers": {k: v for h in ok for k, v in h["markers"].items()},
            "streams": [s for h in ok for s in h["streams"]]}


@app.get("/health")
async def health(request: Request):
    full = _merge_health(await _node_health())
    return full if _operator(request) else {"ok": full["ok"]}


@app.get("/markers/{token}")
async def markers(token: str, request: Request):
    bodies, resp = await _fan(request, f"/markers/{token}", targets=[(n, dict(request.query_params)) for n in NODES])
    return resp or {"databases": sum(b["databases"] for b in bodies), "applied": sum(b["applied"] for b in bodies),
                    "missing": [m for b in bodies for m in b["missing"]]}


def _merge_audits(bodies):
    bodies = [b for b in bodies if b]
    if not bodies:
        return {}
    return {"at": min(b["at"] for b in bodies), "ok": all(b["ok"] for b in bodies),
            "databases": sum(b["databases"] for b in bodies),
            "tenants": {k: v for b in bodies for k, v in b["tenants"].items()},
            "localized": [l for b in bodies for l in b["localized"]],
            "took_ms": max(b["took_ms"] for b in bodies), "sql_ms": round(sum(b["sql_ms"] for b in bodies), 1),
            "invisible_txns": sum(b["invisible_txns"] for b in bodies), "snapshot": f"one per database, {len(bodies)} nodes"}


@app.get("/admin/audit")
async def audit_last(request: Request):
    bodies, resp = await _fan(request, "/admin/audit", targets=[(n, {}) for n in NODES])
    return resp or _merge_audits(bodies)


@app.post("/admin/audit")
async def audit_now(request: Request):
    bodies, resp = await _fan(request, "/admin/audit", "POST", targets=[(n, dict(request.query_params)) for n in NODES])
    return resp or _merge_audits(bodies)


async def _by_tenant_param(request, path, method):
    tid = request.query_params.get("tenant")
    node = await _node_for(int(tid)) if tid and tid.isdigit() else None
    return _relay(await _call(node or NODES[0], method, path, request, dict(request.query_params)))


@app.post("/admin/corrupt")
async def corrupt(request: Request):
    return await _by_tenant_param(request, "/admin/corrupt", "POST")


@app.post("/admin/keys/rotate")
async def rotate_key(request: Request):
    return await _by_tenant_param(request, "/admin/keys/rotate", "POST")


@app.post("/admin/rebuild")
async def rebuild(request: Request):
    if request.query_params.get("tenant"):
        return await _by_tenant_param(request, "/admin/rebuild", "POST")
    bodies, resp = await _fan(request, "/admin/rebuild", "POST", targets=[(n, {}) for n in NODES])
    return resp or {"rebuilt": [d for b in bodies for d in b["rebuilt"]], "took_s": max(b["took_s"] for b in bodies)}


@app.post("/admin/discover")
async def discover(request: Request):
    bodies, resp = await _fan(request, "/admin/discover", "POST", targets=[(n, {}) for n in NODES])
    if resp:
        return resp
    try:
        await refresh_routes()  # route the new stores at once
    except Exception as e:
        log.warning("route refresh failed: %s", e)
    return {"added": [d for b in bodies for d in b["added"]], "databases": sum(b["databases"] for b in bodies)}


@app.post("/admin/snapshot")
async def snapshot(request: Request):
    bodies, resp = await _fan(request, "/admin/snapshot", "POST", targets=[(n, {}) for n in NODES])
    return resp or {"saved": sum(b["saved"] for b in bodies), "skipped": sum(b["skipped"] for b in bodies),
                    "seconds": max(b["seconds"] for b in bodies)}


@app.get("/admin/keys")
async def keys(request: Request):
    targets = await _targets(request, "/admin/keys") if request.query_params.get("stores") else [(n, {}) for n in NODES]
    bodies, resp = await _fan(request, "/admin/keys", targets=targets)
    return resp or sorted((k for b in bodies for k in b), key=lambda k: k["tenant_id"])


def _pct(xs, p):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(p / 100 * len(xs)))], 3) if xs else None


@app.get("/monitor.json")
async def monitor_json(request: Request):
    bodies, resp = await _fan(request, "/monitor.json", targets=[(n, {}) for n in NODES])
    if resp:
        return resp
    h = _merge_health(await _node_health())
    h.pop("streams")
    h.pop("markers")
    audits = [b["audit"] for b in bodies if b.get("audit")]
    audit = None
    if audits:
        audit = {"at": min(a["at"] for a in audits), "ok": all(a["ok"] for a in audits),
                 "databases": sum(a["databases"] for a in audits), "took_ms": max(a["took_ms"] for a in audits),
                 "sql_ms": round(sum(a["sql_ms"] for a in audits), 1),
                 "invisible_txns": sum(a["invisible_txns"] for a in audits), "snapshot": f"{len(audits)} nodes",
                 "localized": [l for a in audits for l in a["localized"]],
                 "tenants": {k: v for a in audits for k, v in a["tenants"].items()},
                 "bad": {k: v for a in audits for k, v in a["bad"].items()}}
    lat = {path: {"n": len(v), "p50_ms": _pct([x for x, _ in v], 50), "p99_ms": _pct([x for x, _ in v], 99),
                  "rejected_429": sum(1 for _, c in v if c == 429)} for path, v in sorted(LAT.items())}
    return {"uptime_s": round(time.time() - STARTED), "health": h, "latency": lat,
            "limits": {k: v for b in bodies for k, v in b["limits"].items()},
            "tiers": {k: v for b in bodies for k, v in b["tiers"].items()}, "audit": audit,
            "audit_history": bodies[0]["audit_history"], "audit_interval_s": bodies[0]["audit_interval_s"],
            "router": {"nodes": NODES, "stores_per_node": {n: sum(1 for v in owner_of.values() if v == n) for n in NODES}}}
