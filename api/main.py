"""Serving layer API. Owner: Member C.

    GET /health                   API up + Postgres reachable (503 if not)
    GET /api/patients             speed view: every patient's current window and vital risk
    GET /api/patients/{id}        merged view: vital risk (speed) + lab risk (batch) -> combined
    GET /api/alerts               alerts from Spark Q3 (filters: patient, severity, type, rule)
    GET /api/metrics              JSON summary: freshness, categories, alert counts, API traffic
    GET /metrics                  Prometheus format (scraped by Prometheus)
    GET /docs                     interactive OpenAPI docs

Run: docker compose up -d api   ->  http://localhost:8000/docs
"""
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest

from api import monitoring
from api.db import DatabaseUnavailable
from api.deps import get_repo
from api.repository import WardRepository
from api.routers import alerts, patients, system
from common.logger import get_logger

log = get_logger("api")

app = FastAPI(
    title="Ward Vitals API",
    version="1.0.0",
    description="Serving layer of the ward vitals Lambda pipeline. Risk rules are simulated "
                "academic rules, not clinical guidance.",
)
app.middleware("http")(monitoring.track_requests)
app.include_router(patients.router)
app.include_router(alerts.router)
app.include_router(system.router)

REGISTRY.register(monitoring.ServingCollector())


@app.exception_handler(DatabaseUnavailable)
def database_unavailable(_request: Request, exc: DatabaseUnavailable) -> JSONResponse:
    log.error("database_unavailable", error=str(exc).splitlines()[0] if str(exc) else "")
    return JSONResponse(status_code=503, content={"detail": "database unavailable"})


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse("/docs")


@app.get("/health", tags=["system"])
def health(repo: WardRepository = Depends(get_repo)) -> JSONResponse:
    """200 when the API can query Postgres, 503 (status degraded) when it cannot."""
    try:
        repo.ping()
    except Exception as exc:  # noqa: BLE001
        return JSONResponse(status_code=503, content={
            "status": "degraded", "postgres": "unavailable",
            "error": str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__,
        })
    return JSONResponse({"status": "ok", "postgres": "ok"})


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics() -> Response:
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
