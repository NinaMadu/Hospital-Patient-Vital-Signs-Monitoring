"""A4 daily vital summary on a small local lake (local Spark, Postgres replaced by a fake)."""
import json
import os
import sys
from datetime import timedelta

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from common import sim_clock  # noqa: E402

KAFKA_SCHEMA = "value binary, partition int, offset long"


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    s = (SparkSession.builder.master("local[1]").appName("test-a4")
         .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="module")
def a4(spark):
    from spark.batch import vital_daily_summary
    return vital_daily_summary


def event(event_id, patient_id, day, second, **vitals):
    e = {"event_id": event_id, "patient_id": patient_id, "bed_id": "BED-01", "heart_rate": 80,
         "spo2": 97.0, "systolic_bp": 120, "diastolic_bp": 80, "temperature": 36.8,
         "timestamp": (sim_clock.day_start(day) + timedelta(seconds=second)).isoformat()}
    e.update(vitals)
    return json.dumps(e)


@pytest.fixture()
def lake(spark, tmp_path):
    """A lake written the way Q2 writes it: archive_records, partitioned by sim_day."""
    from spark.streaming import q2_archive

    payloads = [
        event("a1", "P001", 3, 10, heart_rate=70, spo2=98.0),
        event("a2", "P001", 3, 20, heart_rate=90, spo2=90.0),         # SpO2 below 92: abnormal
        event("a2", "P001", 3, 20, heart_rate=90, spo2=90.0),         # same event again
        event("a3", "P001", 3, 30, heart_rate=80, systolic_bp=85),    # SBP below 90: abnormal
        event("b1", "P002", 3, 40, temperature=38.5),                 # fever: abnormal
        event("b2", "P002", 3, 50, spo2=None),                        # invalid: not counted
        event("c1", "P001", 4, 10, heart_rate=150),                   # another day
    ]
    raw = spark.createDataFrame([(p.encode(), 0, i) for i, p in enumerate(payloads)], KAFKA_SCHEMA)
    path = str(tmp_path / "lake")
    q2_archive.archive_records(raw).write.partitionBy("sim_day").parquet(path)
    return path


def rows_by_patient(a4, spark, lake, day):
    from spark.streaming.q2_archive import read_lake

    df = a4.daily_summary(read_lake(spark, day, valid_only=True, path=lake))
    return {r.patient_id: r.asDict() for r in df.collect()}


def test_summary_per_patient_for_one_day(a4, spark, lake):
    rows = rows_by_patient(a4, spark, lake, 3)
    assert set(rows) == {"P001", "P002"}
    p1 = rows["P001"]
    assert p1["sim_day"] == 3
    assert p1["reading_count"] == 3                     # the repeated a2 is counted once
    assert p1["abnormal_count"] == 2
    assert (p1["min_heart_rate"], p1["avg_heart_rate"], p1["max_heart_rate"]) == (70, 80, 90)
    assert p1["min_spo2"] == 90.0 and p1["min_systolic_bp"] == 85
    p2 = rows["P002"]
    assert (p2["reading_count"], p2["abnormal_count"], p2["max_temperature"]) == (1, 1, 38.5)


def test_other_days_are_not_mixed_in(a4, spark, lake):
    assert rows_by_patient(a4, spark, lake, 4)["P001"]["max_heart_rate"] == 150
    assert rows_by_patient(a4, spark, lake, 5) == {}


def test_columns_match_the_table(a4):
    schema = open("database/init/01_schema.sql", encoding="utf-8").read()
    table = schema.split("CREATE TABLE IF NOT EXISTS vital_daily_summary")[1].split(");")[0]
    for col in a4.SUMMARY_COLUMNS:
        assert f"\n    {col} " in table, col


class FakeConn:
    """Records statements; `with conn` commits on success and rolls back on error, like psycopg2."""

    def __init__(self, fail_insert=False):
        self.calls, self.fail_insert = [], fail_insert
        self.committed = self.rolled_back = self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        self.rolled_back, self.committed = exc_type is not None, exc_type is None

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def execute(self, sql, params):
        self.conn.calls.append((sql, params))

    def executemany(self, sql, rows):
        if self.conn.fail_insert:
            raise RuntimeError("insert failed")
        self.conn.calls.append((sql, list(rows)))


def test_run_replaces_the_day_in_one_transaction(a4, spark, lake, monkeypatch):
    conn = FakeConn()
    monkeypatch.setattr("psycopg2.connect", lambda dsn: conn)
    assert a4.run(spark, 3, lake_path=lake) == 2
    (delete, day), (insert, rows) = conn.calls
    assert delete == a4.DELETE_SQL and day == (3,)
    assert insert == a4.INSERT_SQL and len(rows) == 2 and all(len(r) == len(a4.SUMMARY_COLUMNS) for r in rows)
    assert conn.committed and conn.closed


def test_failed_insert_rolls_back_the_delete(a4):
    conn = FakeConn(fail_insert=True)
    with pytest.raises(RuntimeError):
        a4.write_day(conn, 3, [("row",)])
    assert conn.rolled_back and not conn.committed


def test_day_without_readings_leaves_the_table_alone(a4, spark, lake, monkeypatch):
    monkeypatch.setattr("psycopg2.connect", lambda dsn: pytest.fail("must not connect"))
    assert a4.run(spark, 9, lake_path=lake) == 0
