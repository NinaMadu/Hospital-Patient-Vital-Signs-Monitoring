"""B8 risk join on small in-memory DataFrames (local Spark, no Postgres)."""
import os
import sys

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

from common.risk_rules import LAB_AVAILABLE, LAB_UNAVAILABLE  # noqa: E402

DAY = 5
SUMMARY_SCHEMA = ("sim_day int, patient_id string, avg_heart_rate double, min_spo2 double, "
                  "max_temperature double, max_systolic_bp double, min_systolic_bp double")
LAB_SCHEMA = ("sim_day int, patient_id string, test_type string, result_value double, "
              "reference_low double, reference_high double")
NORMAL_VITALS = (75.0, 97.0, 36.8, 125.0, 110.0)


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    s = (SparkSession.builder.master("local[1]").appName("test-risk-consolidation")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="module")
def result(spark):
    from spark.batch.risk_consolidation import consolidate

    patients = spark.createDataFrame([(f"P00{i}",) for i in range(1, 6)], "patient_id string")
    summary = spark.createDataFrame([
        (DAY, "P001", *NORMAL_VITALS),
        (DAY, "P002", 130.0, 90.0, 36.9, 125.0, 110.0),     # HR high (+1), SpO2 low (+2)
        (DAY, "P003", *NORMAL_VITALS),
        (DAY, "P099", *NORMAL_VITALS),                      # not in the ward list: still reported
        (DAY - 1, "P004", 140.0, 85.0, 39.0, 170.0, 80.0),  # other day: ignored
        # P004 and P005: no vitals for day 5
    ], SUMMARY_SCHEMA)
    labs = spark.createDataFrame([
        (DAY, "P001", "potassium", 4.2, 3.5, 5.1),
        (DAY - 1, "P001", "crp", 20.0, 0.0, 5.0),           # older than P001's newest day: ignored
        (DAY, "P002", "crp", 10.0, 0.0, 5.0),               # far high: +2
        (DAY, "P002", "potassium", 5.5, 3.5, 5.1),          # high: +1
        (DAY, "P002", "haemoglobin", 140.0, 120.0, 170.0),  # normal
        (DAY - 1, "P003", "lactate", 2.5, 0.5, 2.0),        # P003 missing today -> yesterday: +1
        (DAY - 2, "P003", "crp", 30.0, 0.0, 5.0),           # too old (age 1): ignored
        (DAY - 2, "P004", "crp", 30.0, 0.0, 5.0),           # only stale labs -> LAB_UNAVAILABLE
        (DAY + 1, "P005", "crp", 30.0, 0.0, 5.0),           # future day: ignored
    ], LAB_SCHEMA)
    rows = consolidate(patients, summary, labs, DAY, max_age_days=1).collect()
    return rows, {r.patient_id: r for r in rows}


def test_one_row_per_patient_with_table_columns(result):
    from spark.batch.risk_consolidation import RISK_COLUMNS

    rows, by_id = result
    assert [r.patient_id for r in rows] == ["P001", "P002", "P003", "P004", "P005", "P099"]
    assert list(rows[0].asDict()) == RISK_COLUMNS
    assert all(r.sim_day == DAY for r in rows)


def test_normal_patient(result):
    r = result[1]["P001"]
    assert (r.vital_risk_score, r.lab_risk_score, r.total_risk_score) == (0, 0, 0)
    assert r.risk_category == "NORMAL" and r.lab_status == LAB_AVAILABLE
    assert r.lab_sim_day == DAY and r.abnormal_labs is None and r.vital_reasons is None


def test_labs_change_the_risk_picture(result):
    r = result[1]["P002"]
    assert r.vital_risk_score == 3 and r.vital_reasons == "spo2_low,heart_rate_high"
    assert r.lab_risk_score == 3 and r.abnormal_labs == "crp:HIGH,potassium:HIGH"
    assert r.total_risk_score == 6 and r.risk_category == "CONCERNING"


def test_missing_today_falls_back_to_yesterdays_labs(result):
    r = result[1]["P003"]
    assert r.lab_sim_day == DAY - 1 and r.lab_risk_score == 1 and r.abnormal_labs == "lactate:HIGH"
    assert r.risk_category == "NORMAL"


def test_no_recent_labs_is_unavailable_never_zero(result):
    for pid in ("P004", "P005"):        # stale labs only / future labs only
        r = result[1][pid]
        assert r.lab_status == LAB_UNAVAILABLE
        assert r.lab_risk_score is None and r.lab_sim_day is None


def test_no_vitals_keeps_patient_with_null_vital_score(result):
    r = result[1]["P004"]
    assert r.vital_risk_score is None and r.total_risk_score == 0 and r.risk_category == "NORMAL"


def test_write_day_replaces_the_day_in_one_transaction():
    from spark.batch.risk_consolidation import DELETE_SQL, INSERT_SQL, write_day

    calls = []

    class Cur:
        def execute(self, sql, params):
            calls.append(("execute", sql, params))

        def executemany(self, sql, rows):
            calls.append(("executemany", sql, len(rows)))

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class Conn:
        def cursor(self):
            return Cur()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, *a):
            calls.append(("rollback" if exc_type else "commit",))
            return False

    write_day(Conn(), DAY, [("row",)] * 15)
    assert calls == [("execute", DELETE_SQL, (DAY,)), ("executemany", INSERT_SQL, 15), ("commit",)]
