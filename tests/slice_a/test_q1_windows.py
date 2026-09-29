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


# ---- streaming behaviour: de-duplication, watermark, window values (A9) ---------------

KAFKA_SCHEMA = "value binary, partition int, offset long"


def run_micro_batch(spark, src, ckpt):
    """Run Q1's streaming path (parse -> valid -> watermark + dedup -> windows) once over
    everything new in `src`; returns the window rows this micro-batch emitted (update mode)."""
    out = []
    raw = spark.readStream.schema(KAFKA_SCHEMA).parquet(str(src))
    valid = (q1.parse(raw).filter("reason IS NULL")
             .select("event_id", "patient_id", *q1.VITALS, "event_time"))
    query = (q1.window_aggregates(q1.deduplicate(valid)).writeStream
             .outputMode("update")
             .foreachBatch(lambda df, _: out.extend(r.asDict() for r in df.collect()))
             .option("checkpointLocation", str(ckpt))
             .trigger(availableNow=True)
             .start())
    query.awaitTermination()
    return out


def append(spark, src, payloads, first_offset):
    rows = [(p.encode(), 0, first_offset + i) for i, p in enumerate(payloads)]
    spark.createDataFrame(rows, KAFKA_SCHEMA).write.mode("append").parquet(str(src))


def by_start(rows):
    return {r["window_start"].replace(tzinfo=timezone.utc): r for r in rows}


def test_stream_drops_duplicates_and_late_events(spark, tmp_path):
    src, ckpt = tmp_path / "src", tmp_path / "ckpt"
    at = lambda s: T0 + timedelta(seconds=s)  # noqa: E731

    # Batch 1: e1 is delivered twice; e3 moves the watermark to T0+200s - 1 min = T0+140s.
    append(spark, src, [event(0, heart_rate=80), event(0, heart_rate=80),
                        event(10, heart_rate=100), event(200)], first_offset=0)
    first = by_start(run_micro_batch(spark, src, ckpt))
    # A reading sits in 4 overlapping 2-minute windows (30 s slide).
    for start in (at(-90), at(-60), at(-30), at(0)):
        w = first[start]
        assert w["reading_count"] == 2                        # the duplicate e1 counted once
        assert (w["min_heart_rate"], w["avg_heart_rate"], w["max_heart_rate"]) == (80, 90, 100)

    # Batch 2: e3 again (duplicate), a reading 3 min late (behind the watermark), one on time.
    append(spark, src, [event(200), event(5, heart_rate=150), event(210)], first_offset=4)
    second = by_start(run_micro_batch(spark, src, ckpt))
    assert all(start > at(0) for start in second)             # the late reading changed nothing
    assert at(90) not in second                                # e3's duplicate changed nothing
    for start in (at(120), at(150), at(180)):                  # windows holding e3 and e5
        assert second[start]["reading_count"] == 2
    assert second[at(210)]["reading_count"] == 1


def test_is_abnormal_uses_project_thresholds(spark):
    payloads = [event(0), event(1, spo2=91.0), event(2, heart_rate=121), event(3, heart_rate=49),
                event(4, temperature=38.1), event(5, systolic_bp=161), event(6, systolic_bp=89),
                event(7, spo2=92.0, heart_rate=120, temperature=38.0, systolic_bp=160)]
    flags = [r.a for r in q1.parse(kafka_rows(spark, payloads))
             .select(q1.is_abnormal().alias("a")).collect()]
    assert flags == [False, True, True, True, True, True, True, False]   # limits themselves are normal
