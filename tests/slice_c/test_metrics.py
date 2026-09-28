"""common/metrics.py: prefixed, idempotent metric helpers and a push that never raises."""
import pytest
from prometheus_client import generate_latest

from common import metrics


def test_counter_is_prefixed_and_reused():
    reg = metrics.new_registry()
    c1 = metrics.counter("widgets", "Widgets made", ["colour"], registry=reg)
    c2 = metrics.counter("widgets", "Widgets made", ["colour"], registry=reg)
    assert c1 is c2
    c1.labels(colour="red").inc(3)
    text = generate_latest(reg).decode()
    assert 'ward_widgets_total{colour="red"} 3.0' in text


def test_gauge_and_histogram():
    reg = metrics.new_registry()
    metrics.gauge("lag_records", "Lag", registry=reg).set(42)
    metrics.histogram("latency_seconds", "Latency", registry=reg, buckets=(0.1, 1)).observe(0.5)
    text = generate_latest(reg).decode()
    assert "ward_lag_records 42.0" in text
    assert 'ward_latency_seconds_bucket{le="1.0"} 1.0' in text


def test_same_name_other_type_is_an_error():
    reg = metrics.new_registry()
    metrics.counter("thing", "x", registry=reg)
    with pytest.raises(ValueError, match="already exists"):
        metrics.gauge("thing", "x", registry=reg)


def test_registries_are_independent():
    a, b = metrics.new_registry(), metrics.new_registry()
    metrics.gauge("only_in_a", "x", registry=a).set(1)
    assert "ward_only_in_a" not in generate_latest(b).decode()


def test_push_failure_returns_false_and_does_not_raise():
    reg = metrics.new_registry()
    metrics.gauge("pushed", "x", registry=reg).set(1)
    assert metrics.push("test_job", reg, gateway="127.0.0.1:1", timeout=0.5) is False


def test_push_success(monkeypatch):
    calls = []
    monkeypatch.setattr(metrics, "push_to_gateway",
                        lambda gw, job, registry, grouping_key, timeout: calls.append((gw, job)))
    assert metrics.push("daily_job", metrics.new_registry(), gateway="pg:9091") is True
    assert calls == [("pg:9091", "daily_job")]


def test_new_registry_never_gets_a_stale_cached_metric():
    # Regression: the cache was keyed by id(registry); CPython reuses ids of collected
    # objects, so a fresh registry could get a gauge bound to a dead one and push nothing.
    import gc

    for _ in range(200):
        reg = metrics.new_registry()
        metrics.gauge("lab_rows_loaded", "x", registry=reg).set(7)
        assert reg.get_sample_value("ward_lab_rows_loaded") == 7
        del reg
        gc.collect()
