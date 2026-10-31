"""ZETA HTTP API (FR-7).

  GET  /tenants/{id}/orders/count?from&to&status
  GET  /tenants/{id}/orders/by-status?from&to
  GET  /tenants/{id}/orders/trend?from&to&granularity=hour|day&status
  GET  /tenants/{id}/products/top?from&to&n=10
  GET  /health
  GET  /monitor?key=<admin key>             live page (B-5)
  GET  /admin/audit                         last audit result
  POST /admin/audit?localize&repair         run a snapshot-exact audit now
  POST /admin/corrupt?tenant=1              demo: damage one hour bucket
  POST /admin/rebuild                       full rebuild from a fresh snapshot (B-2)

Tenant routes need `X-API-Key` (a key bound to that tenant, or the admin key); see zeta/guard.py.
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

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse

from . import guard
from .audit import Auditor
from .cdc import CDC, lsn_str, ts_to_us
from .engine import DAY, HOUR, S_IDX, STATUSES, Engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("zeta")

DSN = os.environ.get("ZETA_DSN", "host=localhost port=5433 user=zeta dbname=zeta")
SLOT = os.environ.get("ZETA_SLOT", "zeta_slot_" + DSN.rsplit("dbname=", 1)[-1].split()[0])
AUDIT_INTERVAL = float(os.environ.get("ZETA_AUDIT_INTERVAL", "60"))
MIN_US, MAX_US = -(2 ** 62), 2 ** 62

engine = Engine()
auditor = Auditor(engine, DSN, AUDIT_INTERVAL)
limiter = guard.Limiter()
cdc: Optional[CDC] = None
LAT = defaultdict(lambda: deque(maxlen=5000))
STARTED = time.time()


@asynccontextmanager
async def lifespan(app):
    global cdc
    cdc = CDC(engine, DSN, SLOT)
    t0 = time.time()
    cdc.bootstrap()
    cdc.start()
    auditor.start()
    log.info("bootstrapped %s orders, %s items, %d tenants in %.1fs",
             engine.stats.get("orders"), engine.stats.get("items"), len(engine.tenants), time.time() - t0)
    log.info("monitor: http://localhost:<port>/monitor?key=%s", guard.admin_key())
    yield
    cdc.stop_evt.set()


app = FastAPI(title="ZETA", lifespan=lifespan)


@app.middleware("http")
async def timing(request: Request, call_next):
    t0 = time.perf_counter()
    resp = await call_next(request)
    route = request.scope.get("route")
    if route is not None and route.path.startswith("/tenants"):
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


def freshness():
    s = engine.stats
    now = time.time()
    return {"as_of_lsn": lsn_str(s["applied_lsn"]),
            "last_commit_at": iso(int(s["last_commit_ts"] * 1e6)) if s["last_commit_ts"] else None,
            "cdc_lag_ms": s["lag_ms"],
            "updated_s_ago": round(now - s["last_apply_wall"], 2) if s["last_apply_wall"] else None}


def _key(request):
    return request.headers.get("x-api-key") or request.query_params.get("key")


def require_admin(request):
    if guard.AUTH and guard.check_key(_key(request)) != "admin":
        raise HTTPException(401, "admin key required")


def admit(request, response, tid, kind, fu, tu, step=HOUR):
    """FR-5: the key must be bound to the tenant in the URL. B-3: charge the query's cost."""
    who = guard.check_key(_key(request)) if guard.AUTH else "admin"
    if who is None:
        raise HTTPException(401, "missing or invalid X-API-Key")
    if who != "admin" and who != tid:
        raise HTTPException(403, "this key is not valid for this tenant")
    if tid not in engine.tenants:
        raise HTTPException(404, f"unknown tenant {tid}")
    cost = engine.tenants[tid].cost(kind, fu, tu, step)
    response.headers["X-ZETA-Cost"] = str(cost)
    if guard.LIMITS and who != "admin":
        ok, left, retry = limiter.charge(tid, engine.tiers.get(tid), cost)
        response.headers["X-RateLimit-Remaining"] = str(int(left))
        if not ok:
            raise HTTPException(429, f"tenant {tid} over its limit (query cost {cost})",
                                headers={"Retry-After": str(retry), "X-ZETA-Cost": str(cost)})


def status_idx(status):
    if status is None:
        return None
    if status not in S_IDX:
        raise HTTPException(400, f"status must be one of {STATUSES}")
    return S_IDX[status]


def envelope(tid, fu, tu, t0, **body):
    return {"tenant_id": tid,
            "from": iso(fu) if fu != MIN_US else None,
            "to": iso(tu) if tu != MAX_US else None,
            **body, "freshness": freshness(),
            "took_ms": round((time.perf_counter() - t0) * 1000, 3)}


@app.get("/tenants/{tid}/orders/count")
def orders_count(tid: int, request: Request, response: Response,
                 from_: Optional[str] = Query(None, alias="from"), to: Optional[str] = None,
                 status: Optional[str] = None):
    t0 = time.perf_counter()
    si = status_idx(status)
    fu, tu = parse_range(from_, to)
    admit(request, response, tid, "count", fu, tu)
    v = engine.count(tid, fu, tu)
    cnt, rev = (int(v[0, si]), int(v[1, si])) if si is not None else (int(v[0].sum()), int(v[1].sum()))
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
    if granularity not in ("hour", "day"):
        raise HTTPException(400, "granularity must be hour or day")
    si = status_idx(status)
    fu, tu = parse_range(from_, to)
    step = HOUR if granularity == "hour" else DAY
    admit(request, response, tid, "trend", fu, tu, step)
    starts, vals = engine.trend(tid, fu, tu, step)
    if si is None:
        cnt, rev = vals[:, 0, :].sum(1), vals[:, 1, :].sum(1)
    else:
        cnt, rev = vals[:, 0, si], vals[:, 1, si]
    buckets = [{"bucket": iso(int(s)), "count": int(c), "revenue_cents": int(r)}
               for s, c, r in zip(starts, cnt, rev)]
    return envelope(tid, fu, tu, t0, granularity=granularity, status=status, buckets=buckets)


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


@app.get("/health")
def health():
    s = dict(engine.stats)
    s["applied_lsn"] = lsn_str(s["applied_lsn"])
    a = auditor.last
    return {"ok": cdc is not None and cdc.error is None and engine.stats["errors"] == 0,
            "cdc_error": cdc.error if cdc else None, "tenants": len(engine.tenants),
            "epoch": iso(engine.epoch), "axis_end": iso(engine.epoch + engine.T * HOUR),
            "audit_ok": a["ok"] if a else None, **s, "freshness": freshness()}


# ---------------- admin / monitoring ----------------

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
def rebuild(request: Request):
    """B-2: full rebuild from a fresh exported snapshot; old state keeps serving until the swap."""
    global cdc
    require_admin(request)
    t0 = time.time()
    old = cdc
    old.stop()
    new = CDC(engine, DSN, SLOT)
    new.bootstrap()
    new.start()
    cdc = new
    return {"rebuilt": True, "took_s": round(time.time() - t0, 2), "orders": engine.stats.get("orders")}


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
    return {"uptime_s": round(time.time() - STARTED), "health": health(), "latency": lat,
            "limits": limiter.snapshot(), "tiers": engine.tiers,
            "audit": None if a is None else {k: a[k] for k in a if k != "tenants"} | {
                "tenants": {tid: v["ok"] for tid, v in a["tenants"].items()},
                "bad": {tid: v for tid, v in a["tenants"].items() if not v["ok"]}},
            "audit_history": auditor.history, "audit_interval_s": AUDIT_INTERVAL}


@app.get("/monitor", response_class=HTMLResponse)
def monitor(request: Request):
    require_admin(request)
    return (Path(__file__).parent / "monitor.html").read_text()
