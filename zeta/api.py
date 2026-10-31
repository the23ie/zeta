"""ZETA HTTP API (FR-7).

Every store (tenant) lives in its own Postgres database; ZETA follows one change stream per database.

Store routes (key bound to that store, or the owner/admin key):
  GET  /tenants/{id}/orders/count?from&to&status
  GET  /tenants/{id}/orders/by-status?from&to
  GET  /tenants/{id}/orders/trend?from&to&granularity=hour|day&status
  GET  /tenants/{id}/products/top?from&to&n=10

Owner routes (the company that owns every store; owner or admin key), aggregated across all store databases:
  GET  /owner/stores
  GET  /owner/orders/count?from&to&status&stores=1,2
  GET  /owner/orders/by-store?from&to&status
  GET  /owner/orders/by-status?from&to&stores
  GET  /owner/orders/trend?from&to&granularity&status&stores
  GET  /owner/products/top?from&to&n&stores
  GET  /dashboard?key=<owner key>           owner's live dashboard

Operations (admin key):
  GET  /health                              per-database streams, freshness
  GET  /monitor?key=<admin key>             live page (B-5)
  GET  /admin/audit | POST /admin/audit?localize&repair
  POST /admin/corrupt?tenant=1              demo: damage one hour bucket
  POST /admin/rebuild[?tenant=7]            rebuild one store database (or all) from a fresh snapshot (B-2)

`from`/`to` are ISO-8601 timestamps (naive = UTC), range is [from, to). Trend buckets are UTC-aligned.
"""
import logging
import os
import random
import re
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse

from . import guard
from .audit import Auditor
from .cdc import Hub, lsn_str, ts_to_us
from .engine import DAY, HOUR, S_IDX, STATUSES, Engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("zeta")

PG = os.environ.get("ZETA_PG", "host=localhost port=5433 user=zeta")
DBS = os.environ.get("ZETA_DBS", "t1m_*")  # one database per store: a prefix pattern or a comma list
AUDIT_INTERVAL = float(os.environ.get("ZETA_AUDIT_INTERVAL", "60"))
MIN_US, MAX_US = -(2 ** 62), 2 ** 62

engine = Engine()
hub = Hub(engine, PG, DBS)
auditor = Auditor(engine, hub, AUDIT_INTERVAL)
limiter = guard.Limiter()
LAT = defaultdict(lambda: deque(maxlen=5000))
STARTED = time.time()


@asynccontextmanager
async def lifespan(app):
    t0 = time.time()
    hub.bootstrap()
    hub.start()
    auditor.start()
    orders = sum(st.boot.get("orders", 0) for st in hub.streams.values())
    log.info("bootstrapped %d store databases (%s), %d stores, %s orders in %.1fs", len(hub.streams), DBS,
             len(engine.tenants), f"{orders:,}", time.time() - t0)
    log.info("monitor: http://localhost:<port>/monitor?key=%s", guard.admin_key())
    log.info("owner dashboard: http://localhost:<port>/dashboard?key=%s", guard.owner_key())
    yield
    hub.stop()


app = FastAPI(title="ZETA", lifespan=lifespan)


@app.middleware("http")
async def timing(request: Request, call_next):
    t0 = time.perf_counter()
    resp = await call_next(request)
    route = request.scope.get("route")
    if route is not None and route.path.startswith(("/tenants", "/owner")):
        LAT[route.path].append(((time.perf_counter() - t0) * 1000, resp.status_code))
    return resp


def iso(us):
    return datetime.fromtimestamp(us / 1e6, timezone.utc).isoformat()


def _fix_plus(s):
    # an unencoded '+' in a query string arrives as ' ': '...T10:00:00 05:30' -> '+05:30'
    return re.sub(r" (\d\d(:?\d\d)?)$", r"+\1", s.replace("Z", "+00:00")) if s else s


def parse_range(f, t):
    f, t = _fix_plus(f), _fix_plus(t)
    try:
        fu = ts_to_us(f) if f else MIN_US
        tu = ts_to_us(t) if t else MAX_US
    except ValueError as e:
        raise HTTPException(400, f"bad timestamp: {e}")
    return fu, tu


def _fresh(st):
    now = time.time()
    return {"database": st.db, "as_of_lsn": lsn_str(st.applied_lsn),
            "last_commit_at": iso(int(st.last_commit_ts * 1e6)) if st.last_commit_ts else None,
            "cdc_lag_ms": st.lag_ms,
            "updated_s_ago": round(now - st.last_apply_wall, 2) if st.last_apply_wall else None}


def freshness(tid):
    """B-1: freshness of the store database this tenant lives in."""
    st = engine.stream_of.get(tid)
    return _fresh(st) if st else None


def owner_freshness(tids):
    """Owner answers are as fresh as the stalest store database they include."""
    fs = [freshness(t) for t in tids if engine.stream_of.get(t)]
    if not fs:
        return None
    ago = [f["updated_s_ago"] for f in fs if f["updated_s_ago"] is not None]
    lag = [f["cdc_lag_ms"] for f in fs if f["cdc_lag_ms"] is not None]
    return {"databases": len({f["database"] for f in fs}), "max_cdc_lag_ms": max(lag) if lag else None,
            "updated_s_ago": min(ago) if ago else None}


def _key(request):
    return request.headers.get("x-api-key") or request.query_params.get("key")


def require_admin(request):
    if guard.AUTH and guard.check_key(_key(request)) != "admin":
        raise HTTPException(401, "admin key required")


def admit(request, response, tid, kind, fu, tu, step=HOUR):
    """FR-5: the key must be bound to the tenant in the URL (the owner owns every store). B-3: charge the cost."""
    who = guard.check_key(_key(request)) if guard.AUTH else "admin"
    if who is None:
        raise HTTPException(401, "missing or invalid X-API-Key")
    if who not in ("admin", "owner") and who != tid:
        raise HTTPException(403, "this key is not valid for this tenant")
    if tid not in engine.tenants:
        raise HTTPException(404, f"unknown tenant {tid}")
    cost = engine.tenants[tid].cost(kind, fu, tu, step)
    response.headers["X-ZETA-Cost"] = str(cost)
    if guard.LIMITS and who != "admin":
        bucket, tier = ("owner", "owner") if who == "owner" else (tid, engine.tiers.get(tid))
        _charge(response, bucket, tier, cost)


def _charge(response, bucket, tier, cost):
    ok, left, retry = limiter.charge(bucket, tier, cost)
    response.headers["X-RateLimit-Remaining"] = str(int(left))
    if not ok:
        raise HTTPException(429, f"{bucket} over its limit (query cost {cost})",
                            headers={"Retry-After": str(retry), "X-ZETA-Cost": str(cost)})


def admit_owner(request, response, kind, fu, tu, stores, step=HOUR):
    who = guard.check_key(_key(request)) if guard.AUTH else "admin"
    if who not in ("admin", "owner"):
        raise HTTPException(401 if who is None else 403, "owner key required")
    if stores:
        try:
            tids = sorted({int(x) for x in stores.split(",") if x.strip()})
        except ValueError:
            raise HTTPException(400, "stores must be a comma list of store ids")
        unknown = [t for t in tids if t not in engine.tenants]
        if unknown:
            raise HTTPException(404, f"unknown stores {unknown}")
    else:
        tids = sorted(engine.tenants)
    cost = round(sum(engine.tenants[t].cost(kind, fu, tu, step) for t in tids), 2)
    response.headers["X-ZETA-Cost"] = str(cost)
    if guard.LIMITS and who == "owner":
        _charge(response, "owner", "owner", cost)
    return tids


def status_idx(status):
    if status is None:
        return None
    if status not in S_IDX:
        raise HTTPException(400, f"status must be one of {STATUSES}")
    return S_IDX[status]


def _span(fu, tu):
    return {"from": iso(fu) if fu != MIN_US else None, "to": iso(tu) if tu != MAX_US else None}


def envelope(tid, fu, tu, t0, **body):
    return {"tenant_id": tid, **_span(fu, tu), **body, "freshness": freshness(tid),
            "took_ms": round((time.perf_counter() - t0) * 1000, 3)}


def owner_envelope(tids, fu, tu, t0, **body):
    return {"owner": True, "stores": len(tids), **_span(fu, tu), **body, "freshness": owner_freshness(tids),
            "took_ms": round((time.perf_counter() - t0) * 1000, 3)}


def _cnt_rev(v, si):
    return (int(v[0, si]), int(v[1, si])) if si is not None else (int(v[0].sum()), int(v[1].sum()))


def _buckets(starts, vals, si):
    if si is None:
        cnt, rev = vals[:, 0, :].sum(1), vals[:, 1, :].sum(1)
    else:
        cnt, rev = vals[:, 0, si], vals[:, 1, si]
    return [{"bucket": iso(int(s)), "count": int(c), "revenue_cents": int(r)} for s, c, r in zip(starts, cnt, rev)]


def _granularity(g):
    if g not in ("hour", "day"):
        raise HTTPException(400, "granularity must be hour or day")
    return HOUR if g == "hour" else DAY


# ---------------- store (tenant) routes ----------------

@app.get("/tenants/{tid}/orders/count")
def orders_count(tid: int, request: Request, response: Response,
                 from_: Optional[str] = Query(None, alias="from"), to: Optional[str] = None,
                 status: Optional[str] = None):
    t0 = time.perf_counter()
    si = status_idx(status)
    fu, tu = parse_range(from_, to)
    admit(request, response, tid, "count", fu, tu)
    cnt, rev = _cnt_rev(engine.count(tid, fu, tu), si)
    return envelope(tid, fu, tu, t0, status=status, count=cnt, revenue_cents=rev)


@app.get("/tenants/{tid}/orders/by-status")
def orders_by_status(tid: int, request: Request, response: Response,
                     from_: Optional[str] = Query(None, alias="from"), to: Optional[str] = None):
    t0 = time.perf_counter()
    fu, tu = parse_range(from_, to)
    admit(request, response, tid, "count", fu, tu)
    v = engine.count(tid, fu, tu)
    return envelope(tid, fu, tu, t0, by_status={s: {"count": int(v[0, i]), "revenue_cents": int(v[1, i])}
                                                for i, s in enumerate(STATUSES)})


@app.get("/tenants/{tid}/orders/trend")
def orders_trend(tid: int, request: Request, response: Response,
                 from_: Optional[str] = Query(None, alias="from"), to: Optional[str] = None,
                 granularity: str = "day", status: Optional[str] = None):
    t0 = time.perf_counter()
    step = _granularity(granularity)
    si = status_idx(status)
    fu, tu = parse_range(from_, to)
    admit(request, response, tid, "trend", fu, tu, step)
    starts, vals = engine.trend(tid, fu, tu, step)
    return envelope(tid, fu, tu, t0, granularity=granularity, status=status, buckets=_buckets(starts, vals, si))


@app.get("/tenants/{tid}/products/top")
def products_top(tid: int, request: Request, response: Response,
                 from_: Optional[str] = Query(None, alias="from"), to: Optional[str] = None,
                 n: int = Query(10, ge=1, le=1000)):
    t0 = time.perf_counter()
    fu, tu = parse_range(from_, to)
    admit(request, response, tid, "top", fu, tu)
    rows = engine.top(tid, fu, tu, n)
    return envelope(tid, fu, tu, t0, n=n, products=[{"product_id": p, "name": nm, "sales_cents": s}
                                                    for p, nm, s in rows])


# ---------------- owner routes: aggregate across every store database ----------------

@app.get("/owner/stores")
def owner_stores(request: Request, response: Response):
    admit_owner(request, response, "count", MIN_US, MIN_US, None)
    return [{"tenant_id": tid, "name": engine.names.get(tid), "tier": engine.tiers.get(tid),
             "database": engine.stream_of[tid].db if tid in engine.stream_of else None}
            for tid in sorted(engine.tenants)]


@app.get("/owner/orders/count")
def owner_count(request: Request, response: Response, from_: Optional[str] = Query(None, alias="from"),
                to: Optional[str] = None, status: Optional[str] = None, stores: Optional[str] = None):
    t0 = time.perf_counter()
    si = status_idx(status)
    fu, tu = parse_range(from_, to)
    tids = admit_owner(request, response, "count", fu, tu, stores)
    per = engine.owner_count(tids, fu, tu)
    cnt, rev = _cnt_rev(sum(per.values(), np.zeros((2, len(STATUSES)), np.int64)), si)
    return owner_envelope(tids, fu, tu, t0, status=status, count=cnt, revenue_cents=rev)


@app.get("/owner/orders/by-store")
def owner_by_store(request: Request, response: Response, from_: Optional[str] = Query(None, alias="from"),
                   to: Optional[str] = None, status: Optional[str] = None, stores: Optional[str] = None):
    t0 = time.perf_counter()
    si = status_idx(status)
    fu, tu = parse_range(from_, to)
    tids = admit_owner(request, response, "count", fu, tu, stores)
    rows = []
    for tid, v in engine.owner_count(tids, fu, tu).items():
        cnt, rev = _cnt_rev(v, si)
        rows.append({"tenant_id": tid, "name": engine.names.get(tid), "tier": engine.tiers.get(tid),
                     "database": engine.stream_of[tid].db, "count": cnt, "revenue_cents": rev})
    rows.sort(key=lambda r: (-r["revenue_cents"], r["tenant_id"]))
    return owner_envelope(tids, fu, tu, t0, status=status, by_store=rows)


@app.get("/owner/orders/by-status")
def owner_by_status(request: Request, response: Response, from_: Optional[str] = Query(None, alias="from"),
                    to: Optional[str] = None, stores: Optional[str] = None):
    t0 = time.perf_counter()
    fu, tu = parse_range(from_, to)
    tids = admit_owner(request, response, "count", fu, tu, stores)
    v = sum(engine.owner_count(tids, fu, tu).values(), np.zeros((2, len(STATUSES)), np.int64))
    return owner_envelope(tids, fu, tu, t0, by_status={s: {"count": int(v[0, i]), "revenue_cents": int(v[1, i])}
                                                       for i, s in enumerate(STATUSES)})


@app.get("/owner/orders/trend")
def owner_trend(request: Request, response: Response, from_: Optional[str] = Query(None, alias="from"),
                to: Optional[str] = None, granularity: str = "day", status: Optional[str] = None,
                stores: Optional[str] = None):
    t0 = time.perf_counter()
    step = _granularity(granularity)
    si = status_idx(status)
    fu, tu = parse_range(from_, to)
    tids = admit_owner(request, response, "trend", fu, tu, stores, step)
    starts, vals = engine.owner_trend(tids, fu, tu, step)
    return owner_envelope(tids, fu, tu, t0, granularity=granularity, status=status,
                          buckets=_buckets(starts, vals, si))


@app.get("/owner/products/top")
def owner_top(request: Request, response: Response, from_: Optional[str] = Query(None, alias="from"),
              to: Optional[str] = None, n: int = Query(10, ge=1, le=1000), stores: Optional[str] = None):
    t0 = time.perf_counter()
    fu, tu = parse_range(from_, to)
    tids = admit_owner(request, response, "top", fu, tu, stores)
    rows = engine.owner_top(tids, fu, tu, n)
    return owner_envelope(tids, fu, tu, t0, n=n, products=[
        {"product_id": p, "name": nm, "sales_cents": s, "tenant_id": tid, "store": engine.names.get(tid)}
        for p, nm, s, tid in rows])


# ---------------- health / admin / monitoring ----------------

@app.get("/health")
def health():
    with hub.lock:
        streams = list(hub.streams.values())
    s = dict(engine.stats)
    s["applied_lsn"] = lsn_str(s["applied_lsn"])
    lags = [st.lag_ms for st in streams if st.lag_ms is not None]
    bad = [st.db for st in streams if st.error]
    a = auditor.last
    return {"ok": hub.error is None and not bad and engine.stats["errors"] == 0,
            "cdc_error": hub.error, "broken_streams": bad, "databases": len(streams), "tenants": len(engine.tenants),
            "orders": sum(st.boot.get("orders", 0) for st in streams),
            "items": sum(st.boot.get("items", 0) for st in streams),
            "bootstrap_s": max((st.boot.get("bootstrap_s", 0) for st in streams), default=None),
            "max_cdc_lag_ms": max(lags) if lags else None, "audit_ok": a["ok"] if a else None,
            **{k: v for k, v in s.items() if k not in ("orders", "items", "bootstrap_s")},
            "markers": {st.db: st.last_marker for st in streams},
            "streams": [st.status() for st in streams]}


@app.get("/markers/{token}")
def marker(token: str):
    """Which store databases' streams have applied this WAL marker (used to wait for 'caught up')."""
    with hub.lock:
        streams = list(hub.streams.values())
    missing = [st.db for st in streams if token not in st.markers]
    return {"databases": len(streams), "applied": len(streams) - len(missing), "missing": missing}


@app.get("/admin/audit")
def audit_last(request: Request):
    require_admin(request)
    return auditor.last or {}


@app.post("/admin/audit")
def audit_now(request: Request, localize: bool = True, repair: bool = False):
    require_admin(request)
    return auditor.run(localize=localize, repair=repair)


@app.post("/admin/corrupt")
def corrupt(request: Request, tenant: int = 1):
    require_admin(request)
    h, start = engine.corrupt(tenant, random.Random())
    log.warning("DEMO: corrupted tenant %s hour %s (%s)", tenant, h, iso(start))
    return {"tenant": tenant, "hour": h, "hour_start": iso(start),
            "what": "added one phantom cancelled order to this hour bucket (Fenwick + raw)"}


@app.post("/admin/rebuild")
def rebuild(request: Request, tenant: Optional[int] = None):
    """B-2: rebuild one store's database (or all) from a fresh exported snapshot; other stores keep streaming."""
    require_admin(request)
    t0 = time.time()
    if tenant is not None and tenant not in engine.stream_of:
        raise HTTPException(404, f"unknown tenant {tenant}")
    dbs = hub.rebuild(engine.stream_of[tenant].db if tenant is not None else None)
    return {"rebuilt": dbs, "took_s": round(time.time() - t0, 2)}


def _pct(xs, p):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(p / 100 * len(xs)))], 3) if xs else None


@app.get("/monitor.json")
def monitor_json(request: Request):
    require_admin(request)
    lat = {path: {"n": len(v), "p50_ms": _pct([x for x, _ in v], 50), "p99_ms": _pct([x for x, _ in v], 99),
                  "rejected_429": sum(1 for _, c in v if c == 429)}
           for path, v in sorted(LAT.items())}
    a = auditor.last
    h = health()
    h.pop("streams")
    h.pop("markers")
    return {"uptime_s": round(time.time() - STARTED), "health": h, "latency": lat,
            "limits": limiter.snapshot(), "tiers": engine.tiers,
            "audit": None if a is None else {k: a[k] for k in a if k not in ("tenants", "snapshots")} | {
                "tenants": {tid: v["ok"] for tid, v in a["tenants"].items()},
                "bad": {tid: v for tid, v in a["tenants"].items() if not v["ok"]}},
            "audit_history": auditor.history, "audit_interval_s": AUDIT_INTERVAL}


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    """The owner's dashboard page; its data calls carry the owner key."""
    return (Path(__file__).parent / "dashboard.html").read_text()


@app.get("/monitor", response_class=HTMLResponse)
def monitor(request: Request):
    require_admin(request)
    return (Path(__file__).parent / "monitor.html").read_text()
