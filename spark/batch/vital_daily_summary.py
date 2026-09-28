"""Batch view: per-patient vital summary for one sim day, recomputed from the Parquet lake.

Owner: Member A (A4).

    lake /data/lake/vitals, sim_day=N, valid events only
        -> drop repeated event_ids
        -> per patient: avg/min/max of the vitals, reading_count, abnormal_count
        -> vital_daily_summary (day N's rows replaced in one transaction)

    docker compose exec spark-master spark-submit /opt/project/spark/batch/vital_daily_summary.py --sim-day 5
    (without --sim-day: the last completed day, current_sim_day() - 1)

Lambda: the speed layer's windows are short-lived and only as good as the events that arrived
in time. This view is recomputed from the immutable master dataset, so it counts every event
and a rerun (or a rule fix) replaces the old answer. B8's risk join reads this table.
"""
from __future__ import annotations

import argparse
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import metrics
from common.config import get_settings
from common.logger import get_logger
from common.sim_clock import current_sim_day
from spark.streaming import q1_windows as q1
from spark.streaming.q2_archive import read_lake

log = get_logger("vital-daily-summary")

# Same order as the table in database/init/01_schema.sql; each is (column, function, vital).
AGGREGATES = [
    ("avg_heart_rate", F.avg, "heart_rate"),
    ("min_heart_rate", F.min, "heart_rate"),
    ("max_heart_rate", F.max, "heart_rate"),
    ("avg_spo2", F.avg, "spo2"),
    ("min_spo2", F.min, "spo2"),
    ("avg_temperature", F.avg, "temperature"),
    ("max_temperature", F.max, "temperature"),
    ("avg_systolic_bp", F.avg, "systolic_bp"),
    ("min_systolic_bp", F.min, "systolic_bp"),
    ("max_systolic_bp", F.max, "systolic_bp"),
    ("avg_diastolic_bp", F.avg, "diastolic_bp"),
]
SUMMARY_COLUMNS = ["sim_day", "patient_id", *[name for name, _, _ in AGGREGATES],
                   "reading_count", "abnormal_count"]

DELETE_SQL = "DELETE FROM vital_daily_summary WHERE sim_day = %s"
INSERT_SQL = (f"INSERT INTO vital_daily_summary ({', '.join(SUMMARY_COLUMNS)}) "
              f"VALUES ({', '.join(['%s'] * len(SUMMARY_COLUMNS))})")


def daily_summary(events: DataFrame) -> DataFrame:
    """Valid lake events -> one row per (sim_day, patient_id).

    The lake keeps every event it received, so an event re-sent by the producer can appear
    twice; dropping repeated event_ids makes the counts exact. abnormal_count uses the same
    thresholds as Q1 (q1.is_abnormal), so both layers agree on what "abnormal" means.
    """
    return (
        events.dropDuplicates(["event_id"])
        .groupBy("sim_day", "patient_id")
        .agg(*[fn(vital).alias(name) for name, fn, vital in AGGREGATES],
             F.count(F.lit(1)).cast("int").alias("reading_count"),
             F.sum(q1.is_abnormal().cast("int")).cast("int").alias("abnormal_count"))
        .select(*SUMMARY_COLUMNS)
    )


def write_day(conn, sim_day: int, rows: list[tuple]) -> None:
    """Replace day sim_day in vital_daily_summary: delete, then insert, in one transaction.

    Readers see either the old rows or the new ones, never a half-written day, and a
    rerun of the same day leaves the table exactly as one run would.
    """
    with conn, conn.cursor() as cur:   # `with conn` commits on success, rolls back on error
        cur.execute(DELETE_SQL, (sim_day,))
        cur.executemany(INSERT_SQL, rows)


def run(spark: SparkSession, sim_day: int, lake_path: str | None = None) -> int:
    """Summarise one sim day into Postgres. Returns the number of patient rows written."""
    rows = [tuple(r[c] for c in SUMMARY_COLUMNS)
            for r in daily_summary(read_lake(spark, sim_day, valid_only=True, path=lake_path))
            .collect()]   # one row per patient: 15 rows
    if not rows:
        # No readings for the day (producer down, or the day has not happened yet). Keep any
        # rows already in the table rather than replacing a good summary with nothing.
        log.warning("no_readings", sim_day=sim_day)
        return 0
    import psycopg2

    conn = psycopg2.connect(get_settings().postgres.dsn)
    try:
        write_day(conn, sim_day, rows)
    finally:
        conn.close()
    return len(rows)


def push_metrics(sim_day: int, rows: int, seconds: float) -> None:
    """Batch jobs push to the Pushgateway (common/metrics.py); a failed push only logs."""
    reg = metrics.new_registry()
    metrics.gauge("vital_summary_rows", "Patient rows written for the day", registry=reg).set(rows)
    metrics.gauge("vital_summary_sim_day", "Sim day summarised", registry=reg).set(sim_day)
    metrics.gauge("vital_summary_duration_seconds", "Job duration", registry=reg).set(seconds)
    metrics.gauge("vital_summary_last_success_timestamp_seconds", "When the job last succeeded",
                  registry=reg).set_to_current_time()
    metrics.push("vital_daily_summary", registry=reg)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Per-patient daily vital summary from the lake")
    p.add_argument("--sim-day", type=int, default=None,
                   help="day to summarise (default: the last completed day)")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    sim_day = args.sim_day if args.sim_day is not None else current_sim_day() - 1
    started = time.monotonic()
    log.info("started", sim_day=sim_day)
    spark = SparkSession.builder.appName(f"vital-daily-summary-day-{sim_day}").getOrCreate()
    try:
        rows = run(spark, sim_day)
    except Exception:
        log.exception("failed", sim_day=sim_day)
        raise
    finally:
        spark.stop()
    seconds = round(time.monotonic() - started, 1)
    push_metrics(sim_day, rows, seconds)
    log.info("finished", sim_day=sim_day, rows=rows, seconds=seconds)
    return 0


if __name__ == "__main__":
    sys.exit(main())
