"""Q1 logic on small static DataFrames (local Spark, no Kafka or Postgres)."""
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from spark.streaming import q1_windows as q1  # noqa: E402

T0 = datetime(2026, 9, 28, 8, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    # Python workers must use this interpreter (on Windows `python` on PATH may be another one).
    os.environ["PYSPARK_PYTHON"] = sys.executable
    s = (SparkSession.builder.master("local[1]").appName("test-q1")
         .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield s
    s.stop()


def event(n=0, **overrides):
    e = {"event_id": f"e{n}", "patient_id": "P001", "bed_id": "BED-01", "heart_rate": 80,
         "spo2": 97.0, "systolic_bp": 120, "diastolic_bp": 80, "temperature": 36.8,
         "timestamp": (T0 + timedelta(seconds=n)).isoformat()}
    e.update(overrides)
    return json.dumps(e)


def kafka_rows(spark, payloads):
    return spark.createDataFrame(
        [(p.encode(), 0, i) for i, p in enumerate(payloads)], "value binary, partition int, offset long")


def reasons(spark, payloads):
    return [r.reason for r in q1.parse(kafka_rows(spark, payloads)).collect()]


def test_valid_event_has_no_reason(spark):
    assert reasons(spark, [event()]) == [None]


def test_each_invalid_kind_gets_its_reason(spark):
    bad_hr = json.loads(event())
    del bad_hr["heart_rate"]
    assert reasons(spark, [
        "{not valid json",
        event(heart_rate="fast"),
        json.dumps(bad_hr),
        event(spo2=None),
        event(timestamp="yesterday"),
        event(spo2=140.0),
        event(heart_rate=300, temperature=10),
    ]) == [
        "malformed_json",
        "wrong_type",
        "missing_or_null:heart_rate",
        "missing_or_null:spo2",
        "bad_timestamp",
        "out_of_range:spo2",
        "out_of_range:heart_rate,temperature",
    ]


def test_dlq_record_keeps_raw_payload_and_key(spark):
    rec = q1.dlq_records(q1.parse(kafka_rows(spark, [event(spo2=140.0)]))).collect()[0]
    body = json.loads(rec.value)
    assert rec.key == "P001"
    assert body["reason"] == "out_of_range:spo2" and json.loads(body["raw"])["spo2"] == 140.0


def test_current_status_picks_full_window_with_latest_reading(spark):
    # Readings every 10 s for 3 minutes, one abnormal SpO2 near the end.
    payloads = [event(n, spo2=88.0 if n == 170 else 97.0) for n in range(0, 180, 10)]
    valid = q1.parse(kafka_rows(spark, payloads)).filter("reason IS NULL")
    # Same aggregation code as the stream; deduplicate() is streaming-only so it is skipped.
    status = q1.current_status(q1.window_aggregates(valid)).collect()
    assert len(status) == 1
    row = status[0]
    last = T0 + timedelta(seconds=170)
    assert row.last_event_time.replace(tzinfo=timezone.utc) == last
    assert row.window_start.replace(tzinfo=timezone.utc) == T0 + timedelta(seconds=60)
    assert row.window_end.replace(tzinfo=timezone.utc) == T0 + timedelta(seconds=180)
    assert row.reading_count == 12 and row.min_spo2 == 88.0 and row.abnormal_count == 1
