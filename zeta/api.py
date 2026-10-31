"""ZETA HTTP API (FR-7).

  GET /tenants/{id}/orders/count?from&to&status
  GET /tenants/{id}/orders/by-status?from&to
  GET /tenants/{id}/orders/trend?from&to&granularity=hour|day&status
  GET /tenants/{id}/products/top?from&to&n=10
  GET /health

`from`/`to` are ISO-8601 timestamps (naive = UTC), range is [from, to).
Trend buckets are UTC-aligned.
"""
import logging
import os
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query

from .cdc import CDC, lsn_str, ts_to_us
from .engine import DAY, HOUR, NS, S_IDX, STATUSES, Engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
log = logging.getLogger("zeta")

DSN = os.environ.get("ZETA_DSN", "host=localhost port=5433 user=zeta dbname=zeta")
SLOT = os.environ.get("ZETA_SLOT", "zeta_slot_" + DSN.rsplit("dbname=", 1)[-1].split()[0])
MIN_US, MAX_US = -(2 ** 62), 2 ** 62

engine = Engine()
cdc: Optional[CDC] = None


@asynccontextmanager
async def lifespan(app):
    global cdc
    cdc = CDC(engine, DSN, SLOT)
    t0 = time.time()
    cdc.bootstrap()
    cdc.start()
    log.info("bootstrapped %s orders, %s items, %d tenants in %.1fs",
             engine.stats.get("orders"), engine.stats.get("items"), len(engine.tenants), time.time() - t0)
    yield
    cdc.stop_evt.set()


app = FastAPI(title="ZETA", lifespan=lifespan)


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


def tenant_or_404(tid):
    if tid not in engine.tenants:
        raise HTTPException(404, f"unknown tenant {tid}")


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
def orders_count(tid: int, from_: Optional[str] = Query(None, alias="from"), to: Optional[str] = None,
                 status: Optional[str] = None):
    t0 = time.perf_counter()
    tenant_or_404(tid)
    si = status_idx(status)
    fu, tu = parse_range(from_, to)
    v = engine.count(tid, fu, tu)
    cnt, rev = (int(v[0, si]), int(v[1, si])) if si is not None else (int(v[0].sum()), int(v[1].sum()))
    return envelope(tid, fu, tu, t0, status=status, count=cnt, revenue_cents=rev)


@app.get("/tenants/{tid}/orders/by-status")
def orders_by_status(tid: int, from_: Optional[str] = Query(None, alias="from"), to: Optional[str] = None):
    t0 = time.perf_counter()
    tenant_or_404(tid)
    fu, tu = parse_range(from_, to)
    v = engine.count(tid, fu, tu)
    return envelope(tid, fu, tu, t0, by_status={s: {"count": int(v[0, i]), "revenue_cents": int(v[1, i])}
                                                for i, s in enumerate(STATUSES)})


@app.get("/tenants/{tid}/orders/trend")
def orders_trend(tid: int, from_: Optional[str] = Query(None, alias="from"), to: Optional[str] = None,
                 granularity: str = "day", status: Optional[str] = None):
    t0 = time.perf_counter()
    tenant_or_404(tid)
    if granularity not in ("hour", "day"):
        raise HTTPException(400, "granularity must be hour or day")
    si = status_idx(status)
    fu, tu = parse_range(from_, to)
    starts, vals = engine.trend(tid, fu, tu, HOUR if granularity == "hour" else DAY)
    if si is None:
        cnt, rev = vals[:, 0, :].sum(1), vals[:, 1, :].sum(1)
    else:
        cnt, rev = vals[:, 0, si], vals[:, 1, si]
    buckets = [{"bucket": iso(int(s)), "count": int(c), "revenue_cents": int(r)}
               for s, c, r in zip(starts, cnt, rev)]
    return envelope(tid, fu, tu, t0, granularity=granularity, status=status, buckets=buckets)


@app.get("/tenants/{tid}/products/top")
def products_top(tid: int, from_: Optional[str] = Query(None, alias="from"), to: Optional[str] = None,
                 n: int = Query(10, ge=1, le=1000)):
    t0 = time.perf_counter()
    tenant_or_404(tid)
    fu, tu = parse_range(from_, to)
    rows = engine.top(tid, fu, tu, n)
    return envelope(tid, fu, tu, t0, n=n, products=[{"product_id": p, "name": nm, "sales_cents": s}
                                                    for p, nm, s in rows])


@app.get("/health")
def health():
    s = dict(engine.stats)
    s["applied_lsn"] = lsn_str(s["applied_lsn"])
    return {"ok": cdc is not None and cdc.error is None and engine.stats["errors"] == 0,
            "cdc_error": cdc.error if cdc else None, "tenants": len(engine.tenants),
            "epoch": iso(engine.epoch), "axis_end": iso(engine.epoch + engine.T * HOUR),
            **s, "freshness": freshness()}
