"""/api/metrics: a JSON summary of the pipeline and of the API's own traffic.

Owner: Member C.
Prometheus reads GET /metrics (text format); this endpoint is the human-readable version.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

from fastapi import APIRouter, Depends

from api import monitoring, views
from api.deps import get_repo
from api.repository import WardRepository
from common.config import get_settings

router = APIRouter(tags=["system"])


@router.get("/api/metrics")
def pipeline_metrics(repo: WardRepository = Depends(get_repo)) -> dict:
    s = get_settings()
    statuses = repo.current_status()
    categories = Counter(views.vital_risk(r)["category"] for r in statuses)
    return {
        "generated_at": datetime.now(timezone.utc),
        "current_sim_day": views.current_sim_day(s.sim_clock.epoch, s.sim_clock.day_seconds),
        "serving_store": repo.freshness(),
        "patients_by_vital_category": {c: categories.get(c, 0)
                                       for c in ("NORMAL", "WATCH", "CONCERNING")},
        "alerts_last_hour": repo.alert_counts(60),
        "api_requests": monitoring.request_counts(),
    }
