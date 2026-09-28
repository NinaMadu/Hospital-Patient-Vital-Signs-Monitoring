"""Prometheus metric helpers (counters, gauges, histograms, Pushgateway push).

Owner: Member C.

Two ways to get metrics to Prometheus:

  * Long-running services (producer, alert consumer) serve /metrics themselves and
    Prometheus scrapes them:
        from common import metrics
        sent = metrics.counter("events_sent", "Vitals sent to Kafka", ["topic"])
        metrics.start_metrics_server(8001)
        sent.labels(topic="patient-vitals").inc()

  * Short-lived jobs (Spark batch, Airflow tasks) and the Spark streaming driver push to
    the Pushgateway, which Prometheus scrapes:
        reg = metrics.new_registry()
        rows = metrics.gauge("lab_rows_loaded", "Rows loaded", registry=reg)
        rows.set(90)
        metrics.push("daily_lab_consolidation", registry=reg)

Every metric name gets the `ward_` prefix, so ward metrics are easy to find in Grafana.
Asking for the same metric twice returns the same object instead of raising
"Duplicated timeseries", so modules can declare metrics at import time safely.
A push that fails (e.g. the monitoring profile is not running) is logged, never raised:
monitoring must not break the pipeline.
"""
from __future__ import annotations

import os
import threading
import time
import weakref
from typing import Iterable

from prometheus_client import (
    REGISTRY,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    push_to_gateway,
    start_http_server,
)

from common.logger import get_logger

PREFIX = "ward_"
PUSHGATEWAY = os.getenv("PUSHGATEWAY_URL", "pushgateway:9091")

_log = get_logger("metrics")
_lock = threading.Lock()
# registry -> {metric name -> metric}. Weak keys: when a private registry (new_registry())
# is garbage-collected its entries go too. Keying by id(registry) was unsafe: CPython
# reuses ids, so a new registry could get a cached metric bound to a dead one.
_metrics: "weakref.WeakKeyDictionary[CollectorRegistry, dict[str, object]]" = weakref.WeakKeyDictionary()
_last_push_error = 0.0


def _full_name(name: str) -> str:
    return name if name.startswith(PREFIX) else PREFIX + name


def _get_or_create(kind, name: str, documentation: str, labelnames: Iterable[str],
                   registry: CollectorRegistry, **kwargs):
    full = _full_name(name)
    with _lock:
        in_registry = _metrics.setdefault(registry, {})
        existing = in_registry.get(full)
        if existing is not None:
            if not isinstance(existing, kind):
                raise ValueError(f"metric {full} already exists as {type(existing).__name__}")
            return existing
        metric = kind(full, documentation, labelnames=tuple(labelnames), registry=registry, **kwargs)
        in_registry[full] = metric
        return metric


def counter(name: str, documentation: str, labelnames: Iterable[str] = (),
            registry: CollectorRegistry = REGISTRY) -> Counter:
    """A value that only goes up (events sent, alerts stored). Exposed as <name>_total."""
    return _get_or_create(Counter, name, documentation, labelnames, registry)


def gauge(name: str, documentation: str, labelnames: Iterable[str] = (),
          registry: CollectorRegistry = REGISTRY) -> Gauge:
    """A value that goes up and down (lag, batch duration, last event time)."""
    return _get_or_create(Gauge, name, documentation, labelnames, registry)


def histogram(name: str, documentation: str, labelnames: Iterable[str] = (),
              registry: CollectorRegistry = REGISTRY,
              buckets: Iterable[float] = Histogram.DEFAULT_BUCKETS) -> Histogram:
    """A distribution (request latency). Exposed as _bucket, _sum and _count series."""
    return _get_or_create(Histogram, name, documentation, labelnames, registry,
                          buckets=tuple(buckets))


def new_registry() -> CollectorRegistry:
    """A private registry for one job, so a push only contains that job's metrics."""
    return CollectorRegistry()


def start_metrics_server(port: int) -> None:
    """Serve the default registry on http://0.0.0.0:<port>/metrics (background thread)."""
    start_http_server(port)
    _log.info("metrics_server_started", port=port)


def push(job: str, registry: CollectorRegistry, grouping_key: dict[str, str] | None = None,
         gateway: str | None = None, timeout: float = 2.0) -> bool:
    """Push a registry to the Pushgateway. Returns False (and logs) instead of raising.

    Failures are logged at most once a minute, so a missing Pushgateway does not flood logs.
    """
    global _last_push_error
    try:
        push_to_gateway(gateway or PUSHGATEWAY, job=job, registry=registry,
                        grouping_key=grouping_key or {}, timeout=timeout)
        return True
    except Exception as exc:  # noqa: BLE001 - monitoring must never break the pipeline
        now = time.monotonic()
        if now - _last_push_error > 60:
            _last_push_error = now
            _log.warning("pushgateway_unavailable", job=job, gateway=gateway or PUSHGATEWAY,
                         error=str(exc))
        return False
