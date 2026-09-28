"""common/risk_rules.py: the Spark Column rules give exactly the same answers as the Python ones.

This is the check that the speed layer (streaming) and the batch layer cannot drift apart.
"""
import itertools
import os
import sys

import pytest

from common.config import load_thresholds
from common.risk_rules import RiskRules

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

R = RiskRules(load_thresholds())


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    s = (SparkSession.builder.master("local[1]").appName("test-risk-rules")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
         .getOrCreate())
    yield s
    s.stop()


def test_vital_points_col_matches_python(spark):
    hrs, spo2s, temps = [None, 49.9, 50.0, 120.0, 120.1], [None, 91.9, 92.0], [None, 38.0, 38.1]
    sbp_pairs = [(None, None), (160.0, 90.0), (161.0, 90.0), (160.0, 89.0), (170.0, 85.0)]
    trends = [None, False, True]
    rows = [(hr, sp, t, hi, lo, tr) for hr, sp, t, (hi, lo), tr
            in itertools.product(hrs, spo2s, temps, sbp_pairs, trends)]
    df = spark.createDataFrame(
        rows, "avg_heart_rate double, min_spo2 double, max_temperature double, "
              "max_systolic_bp double, min_systolic_bp double, trend boolean")
    got = df.withColumn("pts", R.vital_points_col(trend="trend")).collect()
    assert len(got) == len(rows)
    for r in got:
        expected = R.vital_points(avg_heart_rate=r.avg_heart_rate, min_spo2=r.min_spo2,
                                  max_temperature=r.max_temperature, max_systolic_bp=r.max_systolic_bp,
                                  min_systolic_bp=r.min_systolic_bp, trend=r.trend).points
        assert r.pts == expected, r


def test_vital_points_col_without_optional_columns(spark):
    # vital_daily_summary may have no min_systolic_bp and no trend column.
    df = spark.createDataFrame([(130.0, 90.0, 37.0, 150.0)],
                               "avg_heart_rate double, min_spo2 double, max_temperature double, "
                               "max_systolic_bp double")
    assert df.select(R.vital_points_col(min_systolic_bp=None).alias("p")).first().p == 3


def test_lab_cols_match_python(spark):
    values = [None, 0.0, 2.3, 2.34, 3.4, 3.5, 5.0, 5.1, 7.5, 7.6, 12.0]
    ranges = [(3.5, 5.1), (0.0, 5.0), (None, 5.0), (3.5, None)]
    rows = [(v, lo, hi) for v in values for lo, hi in ranges]
    df = spark.createDataFrame(rows, "result_value double, reference_low double, reference_high double")
    got = df.select("*", R.lab_points_col().alias("pts"), R.lab_flag_col().alias("flag")).collect()
    for r in got:
        assert r.pts == R.lab_points(r.result_value, r.reference_low, r.reference_high), r
        assert r.flag == R.lab_flag(r.result_value, r.reference_low, r.reference_high), r


def test_category_col_matches_python(spark):
    totals = [None, 0, 1, 2, 3, 4, 5, 12]
    df = spark.createDataFrame([(t,) for t in totals], "total_risk_score int")
    got = {r.total_risk_score: r.cat for r in df.select("*", R.category_col().alias("cat")).collect()}
    assert got == {t: R.category(t) for t in totals}


def test_vital_reasons_col_matches_python(spark):
    hrs, spo2s, temps = [None, 49.9, 50.0, 120.1], [None, 91.9, 92.0], [None, 38.1]
    sbp_pairs = [(None, None), (161.0, 90.0), (160.0, 89.0), (170.0, 85.0), (None, 85.0)]
    trends = [None, False, True]
    rows = [(hr, sp, t, hi, lo, tr) for hr, sp, t, (hi, lo), tr
            in itertools.product(hrs, spo2s, temps, sbp_pairs, trends)]
    df = spark.createDataFrame(
        rows, "avg_heart_rate double, min_spo2 double, max_temperature double, "
              "max_systolic_bp double, min_systolic_bp double, trend boolean")
    for r in df.withColumn("reasons", R.vital_reasons_col(trend="trend")).collect():
        expected = R.vital_points(avg_heart_rate=r.avg_heart_rate, min_spo2=r.min_spo2,
                                  max_temperature=r.max_temperature, max_systolic_bp=r.max_systolic_bp,
                                  min_systolic_bp=r.min_systolic_bp, trend=r.trend).reasons
        assert r.reasons == ",".join(expected), r
