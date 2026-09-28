"""spark/streaming/listener.py: progress -> metrics, including Kafka lag (no Spark session)."""
from types import SimpleNamespace

import pytest

pytest.importorskip("pyspark")
from spark.streaming import listener  # noqa: E402


def test_kafka_lag_sums_partitions():
    end = '{"patient-vitals":{"0":100,"1":50,"2":10}}'
    latest = '{"patient-vitals":{"0":110,"1":50,"2":15}}'
    assert listener.kafka_lag(end, latest) == 15


def test_kafka_lag_unknown_when_offsets_missing_or_bad():
    assert listener.kafka_lag(None, '{"t":{"0":1}}') is None
    assert listener.kafka_lag("not json", '{"t":{"0":1}}') is None


def progress(**over):
    source = SimpleNamespace(endOffset='{"patient-vitals":{"0":5}}',
                             latestOffset='{"patient-vitals":{"0":9}}')
    state = SimpleNamespace(numRowsTotal=120, numRowsDroppedByWatermark=2)
    p = dict(name="q3_sustained_alerts", id="abc", batchId=7, numInputRows=40,
             inputRowsPerSecond=4.0, processedRowsPerSecond=float("nan"),
             durationMs={"triggerExecution": 850}, batchDuration=900,
             sources=[source], stateOperators=[state, state],
             eventTime={"watermark": "2026-09-28T08:00:00.000Z"})
    p.update(over)
    return SimpleNamespace(**p)


def test_progress_metrics():
    m = listener.progress_metrics(progress())
    assert m["query"] == "q3_sustained_alerts" and m["batch_id"] == 7
    assert m["input_rows"] == 40 and m["input_rows_per_second"] == 4.0
    assert m["processed_rows_per_second"] == 0.0            # NaN before any data -> 0
    assert m["batch_duration_ms"] == 850 and m["kafka_lag_records"] == 4
    assert m["state_rows"] == 240 and m["rows_dropped_by_watermark"] == 4


def test_progress_without_name_or_offsets():
    m = listener.progress_metrics(progress(name=None, sources=[], stateOperators=[], durationMs=None))
    assert m["query"] == "abc" and m["kafka_lag_records"] is None
    assert m["batch_duration_ms"] == 900 and m["state_rows"] == 0
