"""Metric definitions shared by the ZETA nodes and the router, so both compute every number with the same code.

Additive metrics (counts and sums) can be added up across stores and nodes exactly. Ratio metrics are never
added: they are recomputed from their additive numerator and denominator.
"""
STATUSES = ("placed", "shipped", "delivered", "cancelled", "returned")
I_CANC, I_RET = STATUSES.index("cancelled"), STATUSES.index("returned")

METRICS = {
    "orders": "orders created in the range (all statuses)",
    "revenue": "sum of order totals, all statuses",
    "net_sales": "order totals excluding cancelled and returned orders",
    "aov": "average order value = revenue / orders",
    "cancel_rate": "cancelled orders / orders",
    "return_rate": "returned orders / orders",
    "cancelled_orders": "number of cancelled orders",
    "returned_orders": "number of returned orders",
    "sales": "item sales (qty x price) excluding cancelled and returned orders; the only metric for category / product",
}
RATIOS = {"aov": ("revenue", "orders"), "cancel_rate": ("cancelled_orders", "orders"),
          "return_rate": ("returned_orders", "orders")}
GROUPS = ["none", "store", "status", "category", "product", "day", "hour_of_day", "weekday"]
WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
KPIS = ("orders", "revenue", "net_sales", "aov", "cancel_rate", "return_rate")


def ratio(metric, num, den):
    if metric == "aov":
        return round(num / den) if den else 0
    return round(num / den, 4) if den else 0.0


def metric_value(counts, revenues, metric):
    """counts / revenues: per-status lists (STATUSES order). KeyError for an unknown metric."""
    n, rev = int(sum(counts)), int(sum(revenues))
    base = {"orders": n, "revenue": rev, "net_sales": rev - int(revenues[I_CANC]) - int(revenues[I_RET]),
            "cancelled_orders": int(counts[I_CANC]), "returned_orders": int(counts[I_RET])}
    if metric in RATIOS:
        num, den = RATIOS[metric]
        return ratio(metric, base[num], base[den])
    return base[metric]


def kpis(counts, revenues):
    return {m: metric_value(counts, revenues, m) for m in KPIS}


def change(cur, prev):
    return {k: (round((cur[k] - prev[k]) / prev[k] * 100, 1) if prev[k] else None) for k in cur}


def status_lists(by_status):
    """{status: {count, revenue_cents}} -> (counts, revenues) lists."""
    return ([by_status[s]["count"] for s in STATUSES], [by_status[s]["revenue_cents"] for s in STATUSES])


def cancel_flag(k, overall):
    if k["orders"] >= 20 and overall["cancel_rate"] and k["cancel_rate"] >= 1.5 * overall["cancel_rate"]:
        return f"cancel rate {k['cancel_rate'] * 100:.1f}% vs {overall['cancel_rate'] * 100:.1f}% overall"
    return None
