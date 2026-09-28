"""Prometheus metrics of the serving layer, exposed on GET /metrics.

Owner: Member C.

  * Request metrics (middleware): ward_api_requests_total{method,route,status} and
    ward_api_request_seconds{route}.
  * Serving-store health, read from Postgres when Prometheus scrapes (every 10 s):
        ward_postgres_up                          1 / 0  -> PostgresDown alert
        ward_serving_status_age_seconds           how old the speed view is
        ward_serving_last_reading_age_seconds     newest vital reading in the speed view
        ward_serving_last_alert_age_seconds       newest stored alert
        ward_serving_patients                     rows in patient_current_status
        ward_patients_by_vital_category{category} NORMAL / WATCH / CONCERNING right now
        ward_current_sim_day, ward_lab_latest_sim_day  -> LabFileLate alert
        ward_pipeline_health_recent_failures      FAIL rows in the last 30 min
    They are computed at scrape time (a custom collector), so they are always current and
    no background job is needed.
"""
from __future__ import annotations

import time
from collections import Counter

from fastapi import Request
from prometheus_client.core import GaugeMetricFamily

from api import deps, views
from common import metrics
from common.config import get_settings

REQUESTS = metrics.counter("api_requests", "HTTP requests served", ["method", "route", "status"])
LATENCY = metrics.histogram("api_request_seconds", "HTTP request latency", ["route"],
                            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))


async def track_requests(request: Request, call_next):
    """HTTP middleware: count and time every request by its route template."""
    started = time.perf_counter()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        path = getattr(route, "path", "unmatched")   # /api/patients/{patient_id}, not P007
        if path != "/metrics":
            REQUESTS.labels(request.method, path, str(status)).inc()
            LATENCY.labels(path).observe(time.perf_counter() - started)


def request_counts() -> list[dict]:
    """The request counter as JSON rows (for /api/metrics)."""
    out = []
    for family in REQUESTS.collect():
        for s in family.samples:
            if s.name.endswith("_total"):
                out.append({**s.labels, "count": int(s.value)})
    return sorted(out, key=lambda r: (r["route"], r["method"], r["status"]))


def _gauge(name: str, doc: str, value, labels: dict | None = None) -> GaugeMetricFamily:
    g = GaugeMetricFamily(name, doc, labels=list(labels or {}))
    g.add_metric(list((labels or {}).values()), float("nan") if value is None else float(value))
    return g


class ServingCollector:
    """Custom Prometheus collector: queries Postgres each time /metrics is scraped."""

    def describe(self):
        return []           # nothing at registration time (Postgres may not be up yet)

    def collect(self):
        s = get_settings()
        yield _gauge("ward_current_sim_day", "Current simulated day",
                     views.current_sim_day(s.sim_clock.epoch, s.sim_clock.day_seconds))
        repo = deps.get_repo()
        try:
            f = repo.freshness()
            statuses = repo.current_status()
        except Exception:  # noqa: BLE001 - DB down is itself the signal
            yield _gauge("ward_postgres_up", "1 if the API can query Postgres", 0)
            return
        yield _gauge("ward_postgres_up", "1 if the API can query Postgres", 1)
        yield _gauge("ward_serving_patients", "Patients in patient_current_status", f["patients"])
        yield _gauge("ward_serving_status_age_seconds", "Seconds since the speed view changed",
                     f["status_age_seconds"])
        yield _gauge("ward_serving_last_reading_age_seconds",
                     "Seconds since the newest reading in the speed view",
                     f["last_reading_age_seconds"])
        yield _gauge("ward_serving_last_alert_age_seconds", "Seconds since the newest stored alert",
                     f["last_alert_age_seconds"])
        yield _gauge("ward_serving_alerts", "Rows in vital_alerts", f["alerts_total"])
        # -1 = no lab file loaded yet, so the LabFileLate rule still fires.
        latest_lab = f["latest_lab_sim_day"]
        yield _gauge("ward_lab_latest_sim_day", "Newest sim_day in lab_results (-1 = none)",
                     -1 if latest_lab is None else latest_lab)
        yield _gauge("ward_pipeline_health_recent_failures",
                     "FAIL rows in pipeline_health in the last 30 minutes", f["recent_failures"])

        cats = Counter(views.vital_risk(r)["category"] for r in statuses)
        family = GaugeMetricFamily("ward_patients_by_vital_category",
                                   "Patients per current vital risk category", labels=["category"])
        for category in ("NORMAL", "WATCH", "CONCERNING"):
            family.add_metric([category], cats.get(category, 0))
        yield family
