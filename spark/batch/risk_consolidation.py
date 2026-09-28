"""Joins the latest lab results with vital_daily_summary and writes daily_patient_risk.

Owner: Member B (B8).

    vital_daily_summary (day N, from A4)  ─┐
                                           ├─► per patient: vital score + lab score
    lab_results (newest day <= N, per      │    (common/risk_rules.py, same rules as
      patient, at most lab_max_age_days    │     the speed layer) -> total -> category
      old)                                ─┘
        -> daily_patient_risk (day N's rows replaced in one transaction)

    docker compose exec spark-master spark-submit /opt/project/spark/batch/risk_consolidation.py --sim-day 5
    (without --sim-day: the last completed day, current_sim_day() - 1)

This answers the second half of the business question: how the latest lab results change the
risk picture. Every patient in the ward gets a row. A patient without recent labs is
LAB_UNAVAILABLE with lab_risk_score NULL (never 0), and the total then counts vitals only.
"""
from __future__ import annotations

import argparse
import sys
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import metrics
from common.config import get_settings
from common.logger import get_logger
from common.risk_rules import (
    LAB_AVAILABLE,
    LAB_UNAVAILABLE,
    category_col,
    lab_flag_col,
    lab_points_col,
    vital_points_col,
    vital_reasons_col,
)
from common.sim_clock import current_sim_day

log = get_logger("risk-consolidation")

# Same order as daily_patient_risk in database/init/01_schema.sql (generated_at has a default).
RISK_COLUMNS = [
    "sim_day", "patient_id", "vital_risk_score", "vital_reasons", "lab_risk_score",
    "lab_sim_day", "total_risk_score", "risk_category", "lab_status", "abnormal_labs",
]
DELETE_SQL = "DELETE FROM daily_patient_risk WHERE sim_day = %s"
INSERT_SQL = (f"INSERT INTO daily_patient_risk ({', '.join(RISK_COLUMNS)}) "
              f"VALUES ({', '.join(['%s'] * len(RISK_COLUMNS))})")


def _empty_to_null(col: F.Column) -> F.Column:
    return F.when(col != "", col)


def latest_labs(labs: DataFrame, sim_day: int, max_age_days: int) -> DataFrame:
    """Per patient: lab score and abnormal tests from their newest lab day in [N - age, N].

    A window (max sim_day per patient) keeps only each patient's newest day, so a patient
    missing from today's file falls back to yesterday's results instead of disappearing.
    """
    newest = Window.partitionBy("patient_id")
    recent = (labs.where(F.col("sim_day").between(sim_day - max_age_days, sim_day))
                  .withColumn("lab_sim_day", F.max("sim_day").over(newest))
                  .where(F.col("sim_day") == F.col("lab_sim_day")))
    flagged = F.when(lab_flag_col().isNotNull(), F.concat_ws(":", "test_type", lab_flag_col()))
    return (recent.groupBy("patient_id", "lab_sim_day")
                  .agg(F.sum(lab_points_col()).cast("int").alias("lab_risk_score"),
                       F.concat_ws(",", F.sort_array(F.collect_list(flagged))).alias("abnormal_labs"))
                  .withColumn("abnormal_labs", _empty_to_null(F.col("abnormal_labs"))))


def vital_scores(summary: DataFrame, sim_day: int) -> DataFrame:
    """Per patient: vital score and reasons from the day's summary (no trend in a daily view)."""
    return (summary.where(F.col("sim_day") == sim_day)
                   .select("patient_id",
                           vital_points_col().alias("vital_risk_score"),
                           _empty_to_null(vital_reasons_col()).alias("vital_reasons")))


def consolidate(patients: DataFrame, summary: DataFrame, labs: DataFrame, sim_day: int,
                max_age_days: int) -> DataFrame:
    """The risk join. Pure DataFrame logic: no I/O, so tests can call it on tiny data.

    patients: one column patient_id (the whole ward). Left joins keep every patient even
    when they have no vitals or no labs; an inner join would silently drop them.
    """
    ward = (patients.select("patient_id")
            .unionByName(summary.where(F.col("sim_day") == sim_day).select("patient_id"))
            .distinct())
    joined = (ward.join(vital_scores(summary, sim_day), "patient_id", "left")
                  .join(latest_labs(labs, sim_day, max_age_days), "patient_id", "left"))
    total = (F.coalesce(F.col("vital_risk_score"), F.lit(0))
             + F.coalesce(F.col("lab_risk_score"), F.lit(0)))
    return (joined
            .withColumn("sim_day", F.lit(sim_day))
            .withColumn("total_risk_score", total.cast("int"))
            .withColumn("risk_category", category_col("total_risk_score"))
            .withColumn("lab_status", F.when(F.col("lab_risk_score").isNull(), F.lit(LAB_UNAVAILABLE))
                                       .otherwise(F.lit(LAB_AVAILABLE)))
            .select(*RISK_COLUMNS)
            .orderBy("patient_id"))


# --------------------------------------------------------------------- I/O --

def read_table(spark: SparkSession, query: str) -> DataFrame:
    """Read a Postgres query through Spark JDBC (driver jar baked into both images)."""
    pg = get_settings().postgres
    return (spark.read.format("jdbc")
            .option("url", pg.jdbc_url)
            .option("query", query)
            .option("user", pg.user)
            .option("password", pg.password)
            .option("driver", "org.postgresql.Driver")
            .load())


def write_day(conn, sim_day: int, rows: list[tuple]) -> None:
    """Replace day sim_day in daily_patient_risk: delete, then insert, in one transaction."""
    with conn, conn.cursor() as cur:
        cur.execute(DELETE_SQL, (sim_day,))
        cur.executemany(INSERT_SQL, rows)


def run(spark: SparkSession, sim_day: int) -> list:
    """Consolidate one sim day into Postgres. Returns the rows written (15, one per patient)."""
    s = get_settings()
    age = s.risk_consolidation.lab_max_age_days
    summary = read_table(spark, f"SELECT * FROM vital_daily_summary WHERE sim_day = {int(sim_day)}")
    labs = read_table(spark, "SELECT sim_day, patient_id, test_type, result_value, reference_low, "
                             "reference_high FROM lab_results "
                             f"WHERE sim_day BETWEEN {int(sim_day) - age} AND {int(sim_day)}")
    patients = spark.createDataFrame(
        [(f"{s.patients.id_prefix}{i:03d}",) for i in range(1, s.patients.count + 1)],
        "patient_id string")

    has_vitals, has_labs = not summary.isEmpty(), not labs.isEmpty()
    if not has_vitals and not has_labs:
        # Nothing to consolidate (upstream jobs have not run): keep any existing rows.
        log.warning("no_input", sim_day=sim_day)
        return []
    if not has_vitals:
        log.warning("no_vital_summary", sim_day=sim_day)   # labs-only risk, still useful
    if not has_labs:
        log.warning("no_recent_labs", sim_day=sim_day, max_age_days=age)

    result = consolidate(patients, summary, labs, sim_day, age).collect()
    import psycopg2

    conn = psycopg2.connect(s.postgres.dsn)
    try:
        write_day(conn, sim_day, [tuple(r[c] for c in RISK_COLUMNS) for r in result])
    finally:
        conn.close()
    return result


def push_metrics(sim_day: int, rows: list, seconds: float) -> None:
    reg = metrics.new_registry()
    by_category = {c: sum(1 for r in rows if r.risk_category == c)
                   for c in ("NORMAL", "WATCH", "CONCERNING")}
    patients = metrics.gauge("risk_patients", "Patients per risk category (last run)",
                             ["category"], registry=reg)
    for category, n in by_category.items():
        patients.labels(category=category).set(n)
    metrics.gauge("risk_lab_unavailable", "Patients with no recent labs (last run)",
                  registry=reg).set(sum(1 for r in rows if r.lab_status == LAB_UNAVAILABLE))
    metrics.gauge("risk_sim_day", "Sim day consolidated", registry=reg).set(sim_day)
    metrics.gauge("risk_duration_seconds", "Job duration", registry=reg).set(seconds)
    metrics.gauge("risk_last_success_timestamp_seconds", "When the job last succeeded",
                  registry=reg).set_to_current_time()
    metrics.push("risk_consolidation", registry=reg)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Daily risk: latest labs joined with the vital summary")
    p.add_argument("--sim-day", type=int, default=None,
                   help="day to consolidate (default: the last completed day)")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    sim_day = args.sim_day if args.sim_day is not None else current_sim_day() - 1
    started = time.monotonic()
    log.info("started", sim_day=sim_day)
    spark = SparkSession.builder.appName(f"risk-consolidation-day-{sim_day}").getOrCreate()
    try:
        rows = run(spark, sim_day)
    except Exception:
        log.exception("failed", sim_day=sim_day)
        raise
    finally:
        spark.stop()
    seconds = round(time.monotonic() - started, 1)
    push_metrics(sim_day, rows, seconds)
    log.info("finished", sim_day=sim_day, rows=len(rows), seconds=seconds,
             concerning=sum(1 for r in rows if r.risk_category == "CONCERNING"),
             lab_unavailable=sum(1 for r in rows if r.lab_status == LAB_UNAVAILABLE))
    return 0


if __name__ == "__main__":
    sys.exit(main())
