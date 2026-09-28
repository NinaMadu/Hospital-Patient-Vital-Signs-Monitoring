"""Q3 alert rules on small static DataFrames (local Spark, no Kafka)."""
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from api import alert_consumer  # noqa: E402
from spark.streaming import q3_alerts as q3  # noqa: E402

T0 = datetime(2026, 9, 28, 8, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    s = (SparkSession.builder.master("local[1]").appName("test-q3")
         .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield s
    s.stop()


def event(n=0, patient="P007", seconds=None, **over) -> str:
    e = {"event_id": f"{patient}-e{n}", "patient_id": patient, "bed_id": "BED-07",
         "heart_rate": 80, "spo2": 97.0, "systolic_bp": 120, "diastolic_bp": 80,
         "temperature": 36.8, "timestamp": (T0 + timedelta(seconds=n if seconds is None else seconds)).isoformat()}
    e.update(over)
    return json.dumps(e)


def readings(spark, payloads):
    raw = spark.createDataFrame([(p.encode(), 0, i) for i, p in enumerate(payloads)],
                                "value binary, partition int, offset long")
    return q3.valid_readings(raw)


# ------------------------------------------------------------- pure rules --

def test_rules_come_from_shared_thresholds():
    rules = {r.name: r for r in q3.alert_rules()}
    assert set(rules) == {"SPO2_LOW", "HEART_RATE_HIGH", "HEART_RATE_LOW", "TEMPERATURE_HIGH",
                          "SYSTOLIC_BP_HIGH", "SYSTOLIC_BP_LOW"}
    assert (rules["SPO2_LOW"].direction, rules["SPO2_LOW"].limit) == ("below", 92)
    assert rules["HEART_RATE_HIGH"].limit == 120 and rules["TEMPERATURE_HIGH"].limit == 38.0


def test_limit_itself_is_not_a_breach_and_null_never_is():
    spo2 = q3.Rule("SPO2_LOW", "spo2", "below", 92)
    assert spo2.breached(91.9) and not spo2.breached(92) and not spo2.breached(None)


def test_alert_contract_matches_the_consumer():
    assert q3.ALERT_COLUMNS + ["detected_at"] == alert_consumer.COLUMNS


# --------------------------------------------------------- threshold alerts --

def test_normal_reading_raises_nothing(spark):
    assert q3.threshold_alerts(readings(spark, [event()])).count() == 0


def test_one_alert_per_broken_limit(spark):
    rows = q3.threshold_alerts(readings(spark, [event(1, spo2=88.0, heart_rate=130)])).collect()
    by_rule = {r.rule: r for r in rows}
    assert set(by_rule) == {"SPO2_LOW", "HEART_RATE_HIGH"}
    spo2 = by_rule["SPO2_LOW"]
    assert spo2.alert_id == "P007-e1:SPO2_LOW" and spo2.severity == "WARNING"
    assert spo2.alert_type == "THRESHOLD" and spo2.metric_value == 88.0 and spo2.threshold == 92.0
    assert spo2.message == "spo2 88.0 below limit 92.0"


def test_invalid_events_never_reach_the_rules(spark):
    assert q3.threshold_alerts(readings(spark, ["{broken", event(2, spo2=140.0)])).count() == 0


# --------------------------------------------------------- sustained alerts --

def test_sustained_alert_needs_three_abnormal_readings_in_one_window(spark):
    # P007: 12 readings in the minute 08:00-08:01, five with low SpO2 (worst 85).
    p7 = [event(n, seconds=n * 5, spo2=[88.0, 87.0, 85.0, 89.0, 90.0][n] if n < 5 else 97.0)
          for n in range(12)]
    # P001: only two low readings -> random spikes, not sustained.
    p1 = [event(n, patient="P001", seconds=n * 5, spo2=88.0 if n < 2 else 97.0) for n in range(12)]
    rows = q3.sustained_alerts(readings(spark, p7 + p1)).collect()
    assert len(rows) == 1
    a = rows[0]
    assert a.patient_id == "P007" and a.rule == "SUSTAINED_SPO2_LOW" and a.severity == "CRITICAL"
    assert a.abnormal_count == 5 and a.reading_count == 12 and a.metric_value == 85.0
    assert a.alert_id == f"P007:SUSTAINED_SPO2_LOW:{int(T0.timestamp())}"
    assert a.window_start.replace(tzinfo=timezone.utc) == T0
    assert a.message == "spo2 below limit 92.0 in 5 of 12 readings (worst 85.0)"


def test_each_window_is_judged_on_its_own(spark):
    # Two low readings at the end of one minute and two at the start of the next: no alert.
    payloads = [event(n, seconds=s, spo2=88.0) for n, s in enumerate([50, 55, 65, 70])]
    assert q3.sustained_alerts(readings(spark, payloads)).count() == 0


# ------------------------------------------------------------------ output --

def test_kafka_record_is_keyed_json_with_every_field(spark):
    row = q3.to_kafka(q3.threshold_alerts(readings(spark, [event(3, spo2=88.0)]))).collect()[0]
    body = json.loads(row.value)
    assert row.key == "P007"
    assert set(q3.ALERT_COLUMNS) - set(body) <= {"window_start", "window_end"}   # NULLs omitted
    assert "detected_at" in body
    # The consumer can read what Q3 writes.
    parsed = dict(zip(alert_consumer.COLUMNS, alert_consumer.parse_alert(row.value)))
    assert parsed["alert_id"] == "P007-e3:SPO2_LOW" and parsed["event_time"].tzinfo is not None
