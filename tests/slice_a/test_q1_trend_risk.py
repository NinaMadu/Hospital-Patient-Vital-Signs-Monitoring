"""Q1 trend and vital risk (A6): plain Python on the driver, no Spark session or Postgres."""
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")
from spark.streaming import q1_windows as q1  # noqa: E402

T0 = datetime(2026, 9, 28, 8, 0, 0, tzinfo=timezone.utc)


def history(patient_id, hr, spo2, last_start=T0):
    """Consecutive windows, oldest first, the last one starting at last_start."""
    n = len(hr)
    return {(patient_id, last_start - q1.SLIDE_DELTA * (n - 1 - i)): (hr[i], spo2[i]) for i in range(n)}


def status_row(**overrides):
    row = {"patient_id": "P001", "window_start": T0, "avg_heart_rate": 80.0, "min_spo2": 96.0,
           "max_temperature": 36.9, "max_systolic_bp": 125.0, "min_systolic_bp": 110.0}
    row.update(overrides)
    return row


def test_settings_are_read_from_config():
    assert q1._seconds("30 seconds") == 30 and q1._seconds("2 minutes") == 120
    assert q1.SLIDE_DELTA.total_seconds() == 30
    assert q1.TREND_WINDOWS == 3


@pytest.mark.parametrize("values, expected", [
    ([80, 84, 90], "RISING"),
    ([90, 84, 80], "FALLING"),
    ([80, 81, 82], "STABLE"),        # rising, but by less than the minimum rise
    ([80, 90, 88], "STABLE"),        # not rising in every step
    ([80, 80, 90], "STABLE"),        # a flat step breaks the run
    ([80, 90], None),                # not enough windows yet
    ([80, None, 90], None),
])
def test_trend_label(values, expected):
    assert q1.trend_label(values, q1.MIN_HR_RISE) == expected


def test_trends_uses_consecutive_windows_ending_at_current():
    h = history("P007", hr=[70, 80, 95, 140], spo2=[97, 96, 94, 88])
    # The oldest window (70) is outside the last 3, but all 3 still rise; SpO2 falls by 6.
    assert q1.trends(h, "P007", T0) == ("RISING", "FALLING")


def test_trends_needs_every_window_in_the_run():
    h = history("P007", hr=[80, 90, 100], spo2=[97, 97, 97])
    del h[("P007", T0 - q1.SLIDE_DELTA)]           # a gap: producer stopped for a while
    assert q1.trends(h, "P007", T0) == (None, None)
    assert q1.trends(h, "P001", T0) == (None, None)  # another patient's history does not count


def test_normal_window_scores_zero():
    row = q1.enrich(status_row(), history("P001", [80, 80.5, 80], [97, 97, 97]))
    assert row["hr_trend"] == "STABLE" and row["spo2_trend"] == "STABLE"
    assert (row["vital_risk_score"], row["vital_risk_category"]) == (0, "NORMAL")


def test_spike_with_trend_is_concerning():
    # SpO2 below 92 (2) + heart rate above 120 (1) + trend (1) = 4 -> CONCERNING.
    row = q1.enrich(status_row(patient_id="P007", avg_heart_rate=140.0, min_spo2=87.0),
                    history("P007", [85, 110, 140], [97, 93, 88]))
    assert row["hr_trend"] == "RISING" and row["spo2_trend"] == "FALLING"
    assert (row["vital_risk_score"], row["vital_risk_category"]) == (4, "CONCERNING")


def test_without_history_there_is_no_trend_point():
    row = q1.enrich(status_row(avg_heart_rate=140.0, min_spo2=87.0), {})
    assert row["hr_trend"] is None and row["spo2_trend"] is None
    assert (row["vital_risk_score"], row["vital_risk_category"]) == (3, "WATCH")


def test_status_columns_match_the_table():
    schema = open("database/init/01_schema.sql", encoding="utf-8").read()
    table = schema.split("CREATE TABLE IF NOT EXISTS patient_current_status")[1].split(");")[0]
    for col in q1.STATUS_COLUMNS:
        assert f"\n    {col} " in table, col
