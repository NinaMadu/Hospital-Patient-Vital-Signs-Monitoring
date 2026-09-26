"""Serving layer API. Owner: Member C.

Skeleton only: /health and /metrics. Day 6 adds /api/patients and /api/alerts;
Day 10 merges the speed view (patient_current_status) with the batch view (daily_patient_risk).
"""
from fastapi import FastAPI
from prometheus_client import make_asgi_app

app = FastAPI(title="Ward Vitals API")
app.mount("/metrics", make_asgi_app())


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
