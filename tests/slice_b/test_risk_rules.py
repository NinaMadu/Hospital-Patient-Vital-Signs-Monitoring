"""Tests for common/risk_rules.py: every threshold boundary (Spark parity: test_risk_rules_spark.py)."""
import copy
from datetime import datetime, timezone

import pytest

from common import risk_rules as rr
from common.config import load_thresholds
from common.risk_rules import LAB_AVAILABLE, LAB_UNAVAILABLE, RiskRules

R = RiskRules(load_thresholds())


# ---- vitals -----------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs, points, reasons",
    [
        ({}, 0, ()),
        ({"min_spo2": 92.0}, 0, ()),                       # at the limit is fine
        ({"min_spo2": 91.9}, 2, ("spo2_low",)),
        ({"avg_heart_rate": 50}, 0, ()),
        ({"avg_heart_rate": 49.9}, 1, ("heart_rate_low",)),
        ({"avg_heart_rate": 120}, 0, ()),
        ({"avg_heart_rate": 120.1}, 1, ("heart_rate_high",)),
        ({"max_temperature": 38.0}, 0, ()),
        ({"max_temperature": 38.1}, 1, ("temperature_high",)),
        ({"max_systolic_bp": 160}, 0, ()),
        ({"max_systolic_bp": 161}, 1, ("systolic_bp_high",)),
        ({"min_systolic_bp": 90}, 0, ()),
        ({"min_systolic_bp": 89}, 1, ("systolic_bp_low",)),
        ({"max_systolic_bp": 170, "min_systolic_bp": 85}, 1, ("systolic_bp_high",)),  # BP once
        ({"trend": True}, 1, ("trend",)),
        ({"trend": None}, 0, ()),
    ],
)
def test_vital_points_boundaries(kwargs, points, reasons):
    assert R.vital_points(**kwargs) == (points, reasons)


def test_vital_points_everything_abnormal():
    s = R.vital_points(avg_heart_rate=140, min_spo2=85, max_temperature=39.0,
                       max_systolic_bp=180, min_systolic_bp=100, trend=True)
    assert s.points == 2 + 1 + 1 + 1 + 1
    assert s.reasons == ("spo2_low", "heart_rate_high", "temperature_high", "systolic_bp_high", "trend")


def test_p007_spike_window_is_concerning():
    # The vitals demo scenario: HR 135-160, SpO2 84-89.
    s = R.vital_points(avg_heart_rate=147, min_spo2=84.5, max_temperature=37.0, max_systolic_bp=125)
    assert s.points == 3 and R.category(s.points) == "WATCH"
    s = R.vital_points(avg_heart_rate=147, min_spo2=84.5, trend=True)
    assert R.category(s.points) == "CONCERNING"


# ---- labs -------------------------------------------------------------------


@pytest.mark.parametrize(
    "value, low, high, points, flag",
    [
        (5.0, 0.0, 5.0, 0, None),       # crp at the upper limit
        (5.1, 0.0, 5.0, 1, "HIGH"),
        (7.5, 0.0, 5.0, 1, "HIGH"),     # exactly 1.5x is not "beyond"
        (7.6, 0.0, 5.0, 2, "HIGH"),
        (3.5, 3.5, 5.1, 0, None),       # potassium at the lower limit
        (3.4, 3.5, 5.1, 1, "LOW"),
        (2.34, 3.5, 5.1, 1, "LOW"),     # 3.5 / 1.5 = 2.333...
        (2.3, 3.5, 5.1, 2, "LOW"),
        (0.0, 0.0, 5.0, 0, None),       # low limit 0 has no "far below"
        (None, 3.5, 5.1, 0, None),
        (4.0, None, 5.1, 0, None),
        (4.0, 3.5, None, 0, None),
    ],
)
def test_lab_points_and_flag(value, low, high, points, flag):
    assert R.lab_points(value, low, high) == points
    assert R.lab_flag(value, low, high) == flag


def test_lab_score_sums_results_and_none_means_no_labs():
    assert R.lab_score([]) is None
    assert R.lab_score([(4.0, 3.5, 5.1), (10.2, 0.0, 5.0), (2.1, 0.5, 2.0)]) == 0 + 2 + 1


def test_lab_simulator_output_matches_the_rules():
    """The simulator's 'far out of range' and the rule's must mean the same thing."""
    from simulators.lab_batch.main import LAB_CATALOGUE, LabSimulator

    sim = LabSimulator(epoch=datetime(2026, 9, 28, tzinfo=timezone.utc), day_seconds=300,
                       tests=list(LAB_CATALOGUE))
    for day in range(10):
        for r in sim.rows_for_day(day):
            pts = R.lab_points(r["result_value"], r["reference_low"], r["reference_high"])
            if r["patient_id"] == "P007" and r["test_type"] in ("crp", "lactate"):
                assert pts == 2


# ---- categories and the serving-layer merge ---------------------------------


@pytest.mark.parametrize(
    "total, cat",
    [(0, "NORMAL"), (1, "NORMAL"), (2, "WATCH"), (3, "WATCH"), (4, "CONCERNING"), (12, "CONCERNING"),
     (None, None)],
)
def test_category_boundaries(total, cat):
    assert R.category(total) == cat


def test_negative_total_is_an_error():
    with pytest.raises(ValueError):
        R.category(-1)


@pytest.mark.parametrize(
    "vital, lab, expected",
    [
        (2, None, (2, "WATCH", LAB_UNAVAILABLE)),     # no labs: never counted as "lab score 0"
        (2, 0, (2, "WATCH", LAB_AVAILABLE)),
        (1, 4, (5, "CONCERNING", LAB_AVAILABLE)),
        (None, None, (0, "NORMAL", LAB_UNAVAILABLE)),
        (None, 3, (3, "WATCH", LAB_AVAILABLE)),
    ],
)
def test_combine(vital, lab, expected):
    assert R.combine(vital, lab) == expected


# ---- config handling --------------------------------------------------------


def _thresholds_with(categories):
    t = copy.deepcopy(dict(load_thresholds()))
    t["categories"] = categories
    return t


@pytest.mark.parametrize(
    "categories",
    [
        {"A": {"min": 1, "max": 2}, "B": {"min": 3}},                      # does not start at 0
        {"A": {"min": 0, "max": 1}, "B": {"min": 3}},                      # gap
        {"A": {"min": 0, "max": 2}, "B": {"min": 2}},                      # overlap
        {"A": {"min": 0}, "B": {"min": 2, "max": 5}},                      # open-ended not last
        {"A": {"min": 0, "max": 1}, "B": {"min": 2, "max": 3}},            # nothing above 3
    ],
)
def test_bad_categories_are_rejected(categories):
    with pytest.raises(ValueError):
        RiskRules(_thresholds_with(categories))


def test_rules_follow_the_yaml():
    t = copy.deepcopy(dict(load_thresholds()))
    t["vitals"] = dict(t["vitals"], spo2_min={"below": 95, "points": 3})
    assert RiskRules(t).vital_points(min_spo2=94).points == 3
    assert R.vital_points(min_spo2=94).points == 0


def test_module_functions_use_project_config():
    assert rr.vital_points(min_spo2=90).points == 2
    assert rr.lab_points(7.6, 0.0, 5.0) == 2
    assert rr.category(4) == "CONCERNING"
    assert rr.combine(1, None).lab_status == LAB_UNAVAILABLE
    assert rr.default_rules() is rr.default_rules()
