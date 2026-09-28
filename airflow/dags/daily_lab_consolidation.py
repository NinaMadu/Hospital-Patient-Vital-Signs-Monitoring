"""Daily batch workflow: wait for lab file -> validate -> load -> Spark summary + consolidation -> report.

Owner: Member B.

    resolve_sim_day ─┬─> wait_for_lab_file -> validate_lab_file -> load_lab_results ─┐
                     └─> wait_for_lake_settle -> vital_daily_summary (Spark, A4) ──────┴─>
                             risk_consolidation (Spark, B8) -> generate_report (HTML + CSV)

The two branches are independent (labs vs vitals), so they run in parallel; the risk join
waits for both. Spark jobs are submitted to the standalone cluster (client mode: the driver
runs in this Airflow container), each capped at 2 cores so they fit next to the streaming app.

Which day?  The DAG runs once per simulated day (every SIM_DAY_SECONDS). A scheduled run
processes the day that ended just before it ran: sim_day(data_interval_end) - 1. A manual
run can pass {"sim_day": N} to process or re-run any day (used in the demo).

Idempotent: loading replaces that day's rows in one transaction, so re-running a day, or an
Airflow retry after a crash, never duplicates lab_results rows.

Failures: every task failure writes a FAIL row to pipeline_health (on_failure_callback),
which the API exposes and Prometheus alerts on. A missing file makes the sensor time out;
a bad file fails validation without retries (retrying cannot fix bad data).

Metrics (Pushgateway job "daily_lab_consolidation"): rows and patients loaded, schema
errors, file arrival time, last loaded sim day, load duration. The Spark jobs push their own.

Slow machines: if a run takes longer than one sim day, max_active_runs=1 + catchup=False make
Airflow skip ahead to the latest day instead of queueing forever; re-run a skipped day with
{"sim_day": N}.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor

from common import lab_feed, metrics, sim_clock
from common.config import get_settings
from common.logger import get_logger

S = get_settings()
DAY = timedelta(seconds=S.sim_clock.day_seconds)
LANDING = S.paths.landing_labs
DAG_ID = "daily_lab_consolidation"
LAKE_SETTLE = timedelta(seconds=S.risk_consolidation.lake_settle_seconds)
SIM_DAY_ARG = "{{ ti.xcom_pull(task_ids='resolve_sim_day') }}"

log = get_logger(DAG_ID)


def _pg_connect():
    import psycopg2

    return psycopg2.connect(S.postgres.dsn)


def _day(ti) -> int:
    return int(ti.xcom_pull(task_ids="resolve_sim_day"))


def _push_metrics(task: str, **values: float) -> None:
    """Push gauges to the Pushgateway; never fails the task (see common/metrics.py).

    A push replaces every metric under the same job + grouping key, so each task pushes
    under its own key ({"task": ...}); otherwise the load push would erase the validate push.
    """
    reg = metrics.new_registry()
    help_text = {
        "lab_rows_loaded": "Lab result rows loaded by the last run",
        "lab_patients_loaded": "Patients with at least one lab result in the last run",
        "lab_schema_errors": "Problems found in the last validated lab file",
        "lab_file_arrival_timestamp_seconds": "When the last processed lab file's marker was written",
        "lab_last_loaded_sim_day": "Newest sim day loaded into lab_results",
        "lab_load_duration_seconds": "Seconds taken to load the last lab file",
    }
    for name, value in values.items():
        metrics.gauge(name, help_text[name], registry=reg).set(value)
    metrics.push(DAG_ID, registry=reg, grouping_key={"task": task})


# ------------------------------------------------------------------ tasks --

def resolve_sim_day(params, data_interval_end, **_) -> int:
    """The sim day this run is for (returned value goes to XCom for the other tasks)."""
    if params.get("sim_day") is not None:
        day, how = int(params["sim_day"]), "param"
    else:
        # Airflow passes a pendulum DateTime; its subtraction returns a pendulum Duration that
        # sim_clock's `// timedelta` cannot divide, so convert to a plain datetime first.
        end = datetime.fromtimestamp(data_interval_end.timestamp(), tz=timezone.utc)
        day, how = sim_clock.sim_day(end) - 1, "schedule"
    if day < 0:
        raise AirflowFailException(f"sim day {day} is before SIM_EPOCH; nothing to process")
    log.info("sim_day_resolved", sim_day=day, source=how)
    return day


def validate_lab_file(ti, **_) -> dict:
    day = _day(ti)
    result = lab_feed.validate_file(
        LANDING, day,
        known_tests=S.lab_simulator.tests,
        patient_id_prefix=S.patients.id_prefix,
        day_start=sim_clock.day_start(day),
        day_end=sim_clock.day_end(day),
    )
    marker = lab_feed.marker_path(LANDING, day)
    arrival = marker.stat().st_mtime if marker.exists() else 0
    _push_metrics("validate", lab_schema_errors=result.error_count,
                  lab_file_arrival_timestamp_seconds=arrival)

    if not result.ok:
        log.error("lab_file_invalid", sim_day=day, error_count=result.error_count,
                  errors=result.errors)
        raise AirflowFailException(
            f"labs_day={day}.csv failed validation ({result.error_count} problems): "
            + " | ".join(result.errors[:5]))
    log.info("lab_file_valid", sim_day=day, rows=len(result.rows), patients=result.patients)
    return {"sim_day": day, "rows": len(result.rows), "patients": result.patients}


def load_lab_results(ti, **_) -> int:
    day = _day(ti)
    # Validate again instead of passing rows through XCom (XCom is for small values).
    # Cheap (under 100 rows), and guarantees we load exactly what was validated.
    result = lab_feed.validate_file(
        LANDING, day, S.lab_simulator.tests, S.patients.id_prefix,
        sim_clock.day_start(day), sim_clock.day_end(day))
    if not result.ok:
        raise AirflowFailException(f"labs_day={day}.csv changed after validation")

    started = time.monotonic()
    conn = _pg_connect()
    try:
        n = lab_feed.load_rows(conn, day, result.rows, source_file=result.path)
        with conn.cursor() as cur:
            cur.execute("SELECT max(sim_day) FROM lab_results")
            latest = cur.fetchone()[0]
        lab_feed.record_health(conn, "lab_file_loaded", "OK", value=n,
                               details={"sim_day": day, "patients": result.patients})
    finally:
        conn.close()

    elapsed = time.monotonic() - started
    _push_metrics("load", lab_rows_loaded=n, lab_patients_loaded=result.patients,
                  lab_last_loaded_sim_day=latest, lab_load_duration_seconds=elapsed)
    log.info("lab_results_loaded", sim_day=day, rows=n, patients=result.patients,
             duration_s=round(elapsed, 3))
    return n


def lake_has_settled(ti, **_) -> bool:
    """True once day N ended LAKE_SETTLE ago, so Q2 has committed day N's last readings.

    Q2 writes the lake every 30 s; summarising right at the boundary could miss the tail
    of the day. Always true at once when re-running an old day.
    """
    return datetime.now(timezone.utc) >= sim_clock.day_end(_day(ti)) + LAKE_SETTLE


def generate_report(ti, **_) -> dict:
    from reports.generate_report import generate

    return generate(_day(ti))


def on_task_failure(context) -> None:
    """Any failed task: a FAIL row in pipeline_health, so the API and Prometheus see it."""
    ti = context["task_instance"]
    try:
        day = ti.xcom_pull(task_ids="resolve_sim_day")
    except Exception:  # noqa: BLE001 - the callback must never raise
        day = None
    error = str(context.get("exception"))[:500]
    log.error("task_failed", task_id=ti.task_id, sim_day=day, try_number=ti.try_number, error=error)
    try:
        conn = _pg_connect()
        try:
            lab_feed.record_health(conn, f"task_failed:{ti.task_id}", "FAIL",
                                   details={"sim_day": day, "run_id": context["run_id"],
                                            "error": error})
        finally:
            conn.close()
    except Exception:  # noqa: BLE001
        log.exception("health_record_failed", task_id=ti.task_id)


# -------------------------------------------------------------------- DAG --

with DAG(
    dag_id=DAG_ID,
    description="Lab file + lake -> lab_results, vital summary, daily_patient_risk, HTML/CSV report",
    start_date=S.sim_clock.epoch,
    schedule=DAY,                       # one run per simulated day
    catchup=False,                      # don't backfill hundreds of past days on first start
    max_active_runs=1,
    dagrun_timeout=3 * DAY,
    params={"sim_day": Param(None, type=["null", "integer"], minimum=0,
                             description="Process this sim day instead of the one that just ended")},
    default_args={
        "owner": "member-b",
        "retries": 2,
        "retry_delay": timedelta(seconds=20),
        "on_failure_callback": on_task_failure,
    },
    tags=["batch", "labs", "member-b"],
) as dag:
    resolve = PythonOperator(task_id="resolve_sim_day", python_callable=resolve_sim_day)

    wait_for_file = FileSensor(
        task_id="wait_for_lab_file",
        fs_conn_id="fs_default",
        # The marker, not the CSV: it is written only after the CSV is complete.
        filepath=f"{LANDING}/labs_day={{{{ ti.xcom_pull(task_ids='resolve_sim_day') }}}}.csv._SUCCESS",
        poke_interval=15,
        timeout=2 * DAY.total_seconds(),  # file is due at the day boundary; 2 days late = missing
        mode="reschedule",               # free the worker slot between pokes
        retries=0,                       # the timeout already covers the waiting
    )

    validate = PythonOperator(task_id="validate_lab_file", python_callable=validate_lab_file)
    load = PythonOperator(task_id="load_lab_results", python_callable=load_lab_results)

    wait_for_lake = PythonSensor(
        task_id="wait_for_lake_settle",
        python_callable=lake_has_settled,
        poke_interval=15,
        timeout=DAY.total_seconds(),
        mode="reschedule",
        retries=0,
    )

    def spark_job(task_id: str, script: str) -> SparkSubmitOperator:
        return SparkSubmitOperator(
            task_id=task_id,
            conn_id="spark_default",
            application=f"/opt/project/spark/batch/{script}",
            application_args=["--sim-day", SIM_DAY_ARG],   # templated: this run's sim day
            name=f"{task_id}-day-{SIM_DAY_ARG}",
            execution_timeout=timedelta(minutes=15),
        )

    vital_summary = spark_job("vital_daily_summary", "vital_daily_summary.py")
    risk = spark_job("risk_consolidation", "risk_consolidation.py")
    report = PythonOperator(task_id="generate_report", python_callable=generate_report)

    resolve >> wait_for_file >> validate >> load
    resolve >> wait_for_lake >> vital_summary
    [load, vital_summary] >> risk >> report
