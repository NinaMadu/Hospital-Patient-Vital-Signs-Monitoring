"""/api/patients: who is at risk right now (speed view), and one patient's merged view.

Owner: Member C.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from api import views
from api.deps import get_repo, stale_after_seconds
from api.repository import WardRepository

router = APIRouter(prefix="/api/patients", tags=["patients"])

PATIENT_ID = r"^P\d{3}$"
Category = Literal["NORMAL", "WATCH", "CONCERNING"]


@router.get("")
def list_patients(
    category: Category | None = Query(None, description="Only patients in this vital risk category"),
    sort: Literal["patient_id", "risk"] = Query("patient_id", description="risk = highest first"),
    repo: WardRepository = Depends(get_repo),
    stale_after: float = Depends(stale_after_seconds),
) -> dict:
    """Every monitored patient's current window, vital risk and recent alert count."""
    patients = [views.patient_summary(r, stale_after) for r in repo.current_status()]
    if category:
        patients = [p for p in patients if p["vital_risk"]["category"] == category]
    if sort == "risk":
        patients.sort(key=lambda p: (-p["vital_risk"]["score"], p["patient_id"]))
    return {
        "generated_at": datetime.now(timezone.utc),
        "count": len(patients),
        "patients": patients,
    }


@router.get("/{patient_id}")
def get_patient(
    patient_id: str = Path(..., pattern=PATIENT_ID, examples=["P007"]),
    repo: WardRepository = Depends(get_repo),
    stale_after: float = Depends(stale_after_seconds),
) -> dict:
    """Merged view (Lambda serving layer): current vital risk + latest lab risk -> combined."""
    status = repo.current_status(patient_id)
    daily_risk = repo.latest_daily_risk(patient_id)
    labs = repo.latest_labs(patient_id)
    alerts = repo.alerts(patient_id=patient_id, since_minutes=60, limit=10)
    if not (status or daily_risk or labs or alerts):
        raise HTTPException(404, f"no data for patient {patient_id}")
    return views.merged_view(patient_id, status[0] if status else None, daily_risk, labs,
                             alerts, stale_after)
