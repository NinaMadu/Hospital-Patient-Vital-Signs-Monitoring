"""/api/alerts: alerts raised by Spark Q3 and stored by the alert consumer.

Owner: Member C.
"""
from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, Query

from api.deps import get_repo
from api.repository import WardRepository

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


@router.get("")
def list_alerts(
    patient_id: str | None = Query(None, pattern=r"^P\d{3}$"),
    severity: Literal["WARNING", "CRITICAL"] | None = None,
    alert_type: Literal["THRESHOLD", "SUSTAINED"] | None = None,
    rule: str | None = Query(None, pattern=r"^[A-Z_]+$", examples=["SPO2_LOW"]),
    since_minutes: int = Query(60, ge=1, le=7 * 24 * 60),
    limit: int = Query(50, ge=1, le=500),
    repo: WardRepository = Depends(get_repo),
) -> dict:
    """Newest alerts first, with optional filters."""
    rows = repo.alerts(patient_id=patient_id, severity=severity, alert_type=alert_type,
                       rule=rule, since_minutes=since_minutes, limit=limit)
    return {"count": len(rows), "alerts": rows}
