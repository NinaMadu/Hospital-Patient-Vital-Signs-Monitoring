"""Serving-layer logic: shapes database rows into API responses and merges the two views.

Owner: Member C.

This is where the Lambda architecture comes together:

    speed view  (patient_current_status, updated every ~10 s by Spark Q1)
        -> vital risk score right now
  + batch view  (daily_patient_risk, or lab_results, written once per sim day by Airflow/Spark)
        -> lab risk score from the latest lab results
  = combined risk score and category  (common.risk_rules.combine)

All scoring goes through common/risk_rules.py (Member B), the same code the Spark jobs
use, so the API never has its own copy of a threshold. The functions here are pure (rows
in, dicts out), which is why they are easy to unit-test.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from common import risk_rules as rr

VITALS = ("heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature")


def _r(x: Any, digits: int = 1):
    return None if x is None else round(float(x), digits)


def current_sim_day(epoch: datetime, day_seconds: int, now: datetime | None = None) -> int:
    """sim_day = floor((now - SIM_EPOCH) / SIM_DAY_SECONDS). TODO(A1): use common.sim_clock."""
    now = now or datetime.now(timezone.utc)
    return math.floor((now - epoch).total_seconds() / day_seconds)


# ---------------------------------------------------------------- speed view --

def vital_risk(row: dict) -> dict:
    """Vital risk of the patient's current window.

    Q1 writes vital_risk_score once step A6 is in; until then (column NULL) the API scores
    the window aggregates itself with the same shared rules. `source` says which happened.
    """
    trend = row.get("hr_trend") == "RISING" or row.get("spo2_trend") == "FALLING"
    score = rr.vital_points(
        avg_heart_rate=row.get("avg_heart_rate"),
        min_spo2=row.get("min_spo2"),
        max_temperature=row.get("max_temperature"),
        max_systolic_bp=row.get("max_systolic_bp"),
        min_systolic_bp=row.get("min_systolic_bp"),
        trend=trend,
    )
    if row.get("vital_risk_score") is not None:
        points = int(row["vital_risk_score"])
        category = row.get("vital_risk_category") or rr.category(points)
        source = "patient_current_status"
    else:
        points, category, source = score.points, rr.category(score.points), "computed_by_api"
    return {"score": points, "category": category, "reasons": list(score.reasons),
            "source": source}


def patient_summary(row: dict, stale_after_s: float) -> dict:
    """One patient_current_status row (+ alert counts) as the API shows it."""
    age = row.get("seconds_since_last_reading")
    return {
        "patient_id": row["patient_id"],
        "sim_day": row.get("sim_day"),
        "last_event_time": row.get("last_event_time"),
        "seconds_since_last_reading": _r(age),
        "stale": age is None or age > stale_after_s,
        "window": {"start": row.get("window_start"), "end": row.get("window_end")},
        "vitals": {v: {"avg": _r(row.get(f"avg_{v}")), "min": _r(row.get(f"min_{v}")),
                       "max": _r(row.get(f"max_{v}"))} for v in VITALS},
        "reading_count": row.get("reading_count"),
        "abnormal_count": row.get("abnormal_count"),
        "trends": {"heart_rate": row.get("hr_trend"), "spo2": row.get("spo2_trend")},
        "vital_risk": vital_risk(row),
        "alerts_last_10m": row.get("alerts_last_10m", 0),
        "critical_alerts_last_10m": row.get("critical_alerts_last_10m", 0),
        "updated_at": row.get("updated_at"),
    }


# ---------------------------------------------------------------- batch view --

def lab_view(daily_risk: dict | None, lab_rows: list[dict]) -> dict:
    """Lab risk from the batch layer.

    Prefers the consolidated daily_patient_risk row (Member B's risk join). If lab_results
    holds a newer day than that table (the join has not run yet), the lab score is computed
    from those results with the shared rules. No labs at all -> LAB_UNAVAILABLE, never 0.
    """
    results = [{
        "test_type": r["test_type"],
        "value": r.get("result_value"),
        "unit": r.get("unit"),
        "reference_low": r.get("reference_low"),
        "reference_high": r.get("reference_high"),
        "flag": rr.lab_flag(r.get("result_value"), r.get("reference_low"), r.get("reference_high")),
        "points": rr.lab_points(r.get("result_value"), r.get("reference_low"),
                                r.get("reference_high")),
        "collected_at": r.get("collected_at"),
    } for r in lab_rows]
    lab_day = lab_rows[0]["sim_day"] if lab_rows else None
    risk_day = daily_risk["sim_day"] if daily_risk else None

    if daily_risk is not None and (lab_day is None or risk_day >= lab_day):
        unavailable = daily_risk.get("lab_status") == rr.LAB_UNAVAILABLE
        score = None if unavailable else daily_risk.get("lab_risk_score")
        sim_day, source = risk_day, "daily_patient_risk"
    elif lab_rows:
        score = rr.lab_score((r["value"], r["reference_low"], r["reference_high"]) for r in results)
        sim_day, source = lab_day, "lab_results"
    else:
        score, sim_day, source = None, None, None

    return {
        "score": score,
        "status": rr.LAB_UNAVAILABLE if score is None else rr.LAB_AVAILABLE,
        "sim_day": sim_day,
        "source": source,
        "abnormal": [f"{r['test_type']}:{r['flag']}" for r in results if r["flag"]],
        "results": results,
    }


# ---------------------------------------------------------------- merged view --

def merged_view(patient_id: str, status_row: dict | None, daily_risk: dict | None,
                lab_rows: list[dict], alerts: list[dict], stale_after_s: float) -> dict:
    """The Lambda serving-layer merge for one patient: speed view + batch view."""
    speed = patient_summary(status_row, stale_after_s) if status_row else None
    labs = lab_view(daily_risk, lab_rows)
    vital_score = speed["vital_risk"]["score"] if speed else None
    combined = rr.combine(vital_score, labs["score"])

    if speed is None:
        vital_status = "NO_DATA"
    elif speed["stale"]:
        vital_status = "STALE"
    else:
        vital_status = "LIVE"

    return {
        "patient_id": patient_id,
        "combined": {
            "vital_risk_score": vital_score,
            "lab_risk_score": labs["score"],
            "total_risk_score": combined.total,
            "risk_category": combined.category,
            "vital_status": vital_status,
            "lab_status": combined.lab_status,
            "lab_sim_day": labs["sim_day"],
        },
        "speed_view": speed,
        "batch_view": {"labs": labs, "daily_patient_risk": daily_risk},
        "recent_alerts": alerts,
    }
