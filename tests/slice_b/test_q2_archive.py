"""Q2 raw archive: transformation on static data, plus a real file-sink run with restarts."""
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

EPOCH = datetime(2026, 9, 28, tzinfo=timezone.utc)
DAY_S = 300
KAFKA_SCHEMA = "value binary, partition int, offset long"


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["SIM_EPOCH"] = EPOCH.isoformat()
    os.environ["SIM_DAY_SECONDS"] = str(DAY_S)
    s = (SparkSession.builder.master("local[1]").appName("test-q2")
         .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="module")
def q2(spark):
    from spark.streaming import q2_archive
    return q2_archive


def event(n, at_seconds, **overrides):
    e = {"event_id": f"e{n}", "patient_id": "P001", "bed_id": "BED-01", "heart_rate": 80,
         "spo2": 97.0, "systolic_bp": 120, "diastolic_bp": 80, "temperature": 36.8,
         "timestamp": (EPOCH + timedelta(seconds=at_seconds)).isoformat()}
    e.update(overrides)
    return json.dumps(e)


def kafka_rows(payloads, first_offset=0):
    return [(p.encode(), 0, first_offset + i) for i, p in enumerate(payloads)]


# ---- transformation ---------------------------------------------------------


def test_archive_records_columns_sim_day_and_invalid_kept(spark, q2):
    raw = spark.createDataFrame(kafka_rows([
        event(0, 10),                        # day 0
        event(1, 2 * DAY_S + 5),             # day 2
        event(2, 20, spo2=140.0),            # out of range -> kept, with a reason
        "{not valid json",                   # malformed -> kept, sim_day from ingest time
    ]), KAFKA_SCHEMA)
    rows = {r.kafka_offset: r for r in q2.archive_records(raw).collect()}

    assert list(q2.archive_records(raw).columns) == q2.ARCHIVE_COLUMNS
    assert rows[0].sim_day == 0 and rows[0].reason is None and rows[0].heart_rate == 80
    assert rows[1].sim_day == 2
    assert rows[2].reason.startswith("out_of_range") and rows[2].sim_day == 0
    assert rows[3].reason == "malformed_json"
    assert rows[3].raw == "{not valid json"          # original payload preserved
    assert rows[3].sim_day is not None                # still lands in a partition
    assert rows[0].kafka_partition == 0 and rows[0].ingested_at is not None


# ---- real file sink, restarts, reading back --------------------------------


def run_once(spark, q2, src, lake, ckpt):
    """One availableNow run: process everything new in `src`, then stop (like a restart)."""
    raw = spark.readStream.schema(KAFKA_SCHEMA).parquet(str(src))
    q = q2.start(raw, path=str(lake), checkpoint=str(ckpt), trigger={"availableNow": True})
    q.awaitTermination()


def test_file_sink_partitions_and_restart_never_duplicates(spark, q2, tmp_path):
    src, lake, ckpt = tmp_path / "src", tmp_path / "lake", tmp_path / "ckpt"

    batch1 = kafka_rows([event(i, 10 + i) for i in range(5)] + [event(5, DAY_S + 1)])
    spark.createDataFrame(batch1, KAFKA_SCHEMA).write.mode("append").parquet(str(src))
    run_once(spark, q2, src, lake, ckpt)

    # Restart with nothing new: the checkpoint says batch 1 is done, so nothing is rewritten.
    run_once(spark, q2, src, lake, ckpt)
    assert q2.read_lake(spark, path=str(lake)).count() == 6

    # New data arrives, then another restart: only the new rows are appended.
    batch2 = kafka_rows([event(10 + i, 2 * DAY_S + i) for i in range(3)], first_offset=6)
    spark.createDataFrame(batch2, KAFKA_SCHEMA).write.mode("append").parquet(str(src))
    run_once(spark, q2, src, lake, ckpt)
    run_once(spark, q2, src, lake, ckpt)

    lake_df = q2.read_lake(spark, path=str(lake))
    assert lake_df.count() == 9
    assert lake_df.select("event_id").distinct().count() == 9

    folders = sorted(p.name for p in lake.iterdir())
    assert folders == ["_spark_metadata", "sim_day=0", "sim_day=1", "sim_day=2"]

    # Partition filter: only that day's rows.
    assert q2.read_lake(spark, sim_day=0, path=str(lake)).count() == 5
    assert q2.read_lake(spark, sim_day=2, path=str(lake)).count() == 3


def test_read_lake_valid_only(spark, q2, tmp_path):
    src, lake, ckpt = tmp_path / "src", tmp_path / "lake", tmp_path / "ckpt"
    rows = kafka_rows([event(0, 10), event(1, 11, spo2=None), "{bad"])
    spark.createDataFrame(rows, KAFKA_SCHEMA).write.mode("append").parquet(str(src))
    run_once(spark, q2, src, lake, ckpt)

    assert q2.read_lake(spark, path=str(lake)).count() == 3
    valid = q2.read_lake(spark, sim_day=0, valid_only=True, path=str(lake)).collect()
    assert [r.event_id for r in valid] == ["e0"]
