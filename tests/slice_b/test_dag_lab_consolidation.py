"""DAG import test: the lab DAG loads without errors and has the expected shape.

Needs Airflow, so it runs inside the airflow container:
    docker compose exec airflow python -m pytest /opt/project/tests/slice_b/test_dag_lab_consolidation.py
"""
from datetime import datetime, timezone
from pathlib import Path

import pytest

# "airflow" alone would match this repo's airflow/ folder (a namespace package) on PYTHONPATH.
pytest.importorskip("airflow.models")
from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[2] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS), include_examples=False)
    assert bag.import_errors == {}
    return bag.get_dag("daily_lab_consolidation")


def test_task_graph(dag):
    def downstream(task_id):
        return sorted(dag.get_task(task_id).downstream_task_ids)

    assert downstream("resolve_sim_day") == ["wait_for_lab_file", "wait_for_lake_settle"]
    assert downstream("wait_for_lab_file") == ["validate_lab_file"]
    assert downstream("validate_lab_file") == ["load_lab_results"]
    assert downstream("wait_for_lake_settle") == ["vital_daily_summary"]
    # The risk join needs both branches: loaded labs and the day's vital summary.
    assert sorted(dag.get_task("risk_consolidation").upstream_task_ids) == [
        "load_lab_results", "vital_daily_summary"]
    assert downstream("risk_consolidation") == ["generate_report"]
    assert downstream("generate_report") == []


def test_spark_jobs_get_this_runs_sim_day(dag):
    for task_id, script in [("vital_daily_summary", "vital_daily_summary.py"),
                            ("risk_consolidation", "risk_consolidation.py")]:
        task = dag.get_task(task_id)
        assert task.application.endswith(f"spark/batch/{script}")
        assert task.application_args == ["--sim-day", "{{ ti.xcom_pull(task_ids='resolve_sim_day') }}"]


def test_sensor_waits_for_the_marker_not_the_csv(dag):
    sensor = dag.get_task("wait_for_lab_file")
    assert sensor.filepath.endswith(".csv._SUCCESS")
    assert sensor.mode == "reschedule"


def test_retries_and_failure_callback(dag):
    for task in dag.tasks:
        assert task.on_failure_callback is not None
    assert dag.get_task("load_lab_results").retries == 2
    assert dag.max_active_runs == 1 and dag.catchup is False


def test_resolve_sim_day_accepts_airflow_pendulum_dates():
    import pendulum

    import daily_lab_consolidation as mod
    from common import sim_clock

    end = pendulum.instance(sim_clock.day_start(10))
    assert mod.resolve_sim_day(params={}, data_interval_end=end) == 9
    assert mod.resolve_sim_day(params={"sim_day": 4}, data_interval_end=end) == 4
    assert isinstance(sim_clock.day_start(10), datetime) and sim_clock.day_start(10).tzinfo == timezone.utc
