"""Demo control centre: one web page to drive and explain the pipeline during the video.

    uvicorn demo.control.server:app --port 8050        (started by demo/docker-compose.demo.yml)
    http://localhost:8050

The page shows the live ward (read through the real serving API), the alerts, the batch
layer's daily risk and the Airflow runs, and has buttons that change what goes into the
pipeline: demo scenarios, malformed readings, a DAG run for any day, a bad lab file.
The controller owns the vital-sign feed while it runs (it replaces `vital-producer`).
"""
from __future__ import annotations

import math
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import requests
from fastapi import Body, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from api.db import Database
from common import metrics
from common.config import get_settings
from common.logger import get_logger
from demo.control import batch
from demo.control.producer import ScenarioProducer

log = get_logger("demo-control")
S = get_settings()
HERE = Path(__file__).parent
API_URL = os.getenv("DEMO_API_URL", "http://api:8000").rstrip("/")
LANDING = os.getenv("DEMO_LANDING_DIR", S.lab_simulator.landing_dir)
REPORTS = os.getenv("DEMO_REPORTS_DIR", S.paths.reports)
PATIENT = re.compile(r"^P\d{3}$")

# Links shown on the page, as opened from the presenter's browser.
TOOLS = {
    "api_docs": "http://localhost:8000/docs",
    "kafka_ui": "http://localhost:8085/ui/clusters/ward-vitals/all-topics/patient-vitals",
    "spark_ui": "http://localhost:4040/StreamingQuery/",
    "airflow": "http://localhost:8080/dags/daily_lab_consolidation/grid",
    "grafana": "http://localhost:3000",
    "prometheus_alerts": "http://localhost:9090/alerts",
}

feed = ScenarioProducer(topic=S.kafka.topics.vitals,
                        producer_factory=lambda: _build_producer())
db = Database(S.postgres.dsn)
queries = batch.BatchQueries(db)
airflow = batch.AirflowClient(os.getenv("DEMO_AIRFLOW_URL", "http://airflow:8080"),
                              os.getenv("AIRFLOW_ADMIN_USER", "admin"),
                              os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin"),
                              timeout=2.5)   # a starting Airflow must not stall the page
actions: list[dict] = []          # the presenter's actions, shown as a timeline


def _build_producer():
    from simulators.vital_producer.main import build_producer
    return build_producer(S.kafka.bootstrap_servers)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Prometheus scrapes vital-producer:8001; the demo container carries that network
    # alias, so the producer metrics and alerts keep working while the demo owns the feed.
    if os.getenv("DEMO_METRICS_PORT", "8001") != "0":
        metrics.start_metrics_server(int(os.getenv("DEMO_METRICS_PORT", "8001")))
    if os.getenv("DEMO_AUTOSTART", "1") == "1":
        feed.start()
        _log_action("Normal ward feed started", "15 patients, one reading every 2-5 s each")
    yield
    feed.close()


app = FastAPI(title="Ward Vitals demo control", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")


def _log_action(title: str, detail: str = "") -> None:
    actions.append({"at": datetime.now(timezone.utc), "title": title, "detail": detail})
    del actions[:-30]
    log.info("demo_action", title=title, detail=detail)


def _sim_clock() -> dict:
    now = datetime.now(timezone.utc)
    elapsed = (now - S.sim_clock.epoch).total_seconds()
    day_s = S.sim_clock.day_seconds
    day = math.floor(elapsed / day_s)
    return {"sim_day": day, "seconds_into_day": elapsed - day * day_s, "day_seconds": day_s,
            "last_complete_day": day - 1}


def _api(path: str, **params):
    try:
        r = requests.get(API_URL + path, params=params, timeout=5)
    except requests.RequestException as exc:
        raise HTTPException(502, f"serving API unreachable: {exc}") from exc
    if r.status_code == 404:
        raise HTTPException(404, r.json().get("detail", "not found"))
    if not r.ok:
        raise HTTPException(502, f"serving API answered {r.status_code}")
    return r.json()


def _safe(fn, default=None):
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 - one broken source must not blank the page
        log.warning("demo_source_failed", source=getattr(fn, "__name__", "?"), error=str(exc)[:200])
        return default


def _track_milestones(patients: list[dict]) -> None:
    """Measure, live, how quickly the pipeline reacts to the running scenario."""
    run = feed.run
    if run is None or datetime.now(timezone.utc).timestamp() < run.starts_at:
        return
    now = datetime.now(timezone.utc).timestamp()
    if run.scenario == "outage":
        if run.stale_after_s is None and patients and all(p["stale"] for p in patients):
            run.stale_after_s = now - run.starts_at
        return
    me = next((p for p in patients if p["patient_id"] == run.patient), None)
    if me and run.concerning_after_s is None and me["vital_risk"]["category"] == "CONCERNING":
        run.concerning_after_s = now - run.starts_at
    if run.first_alert_after_s is None:
        since = datetime.fromtimestamp(run.starts_at, timezone.utc)
        first = _safe(lambda: queries.first_alert_since(run.patient, since))
        if first is not None:
            run.first_alert_after_s = max(first.timestamp() - run.starts_at, 0.0)


# -------------------------------------------------------------------- page --

@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(HERE / "static" / "index.html")


@app.get("/reports/{name}", include_in_schema=False)
def report(name: str) -> FileResponse:
    if not batch.REPORT_NAME.match(name):
        raise HTTPException(404)
    path = Path(REPORTS) / name
    if not path.is_file():
        raise HTTPException(404)
    return FileResponse(path)


# ------------------------------------------------------------ read models --

@app.get("/demo/api/state")
def state() -> dict:
    api_health = _safe(lambda: requests.get(API_URL + "/health", timeout=3).json(),
                       {"status": "down"})
    feed_status = feed.status()
    last_ack = feed_status["seconds_since_last_ack"]
    return {
        "now": datetime.now(timezone.utc),
        "clock": _sim_clock(),
        "feed": feed_status,
        "services": {
            "api": api_health.get("status") == "ok",
            "postgres": api_health.get("postgres") == "ok",
            # Kafka acknowledged a reading recently (paused feed: unknown, shown grey)
            "kafka": None if feed_status["paused"] else last_ack is not None and last_ack < 15,
            "airflow": airflow.healthy(),
        },
        "actions": actions[::-1],
        "tools": TOOLS,
    }


@app.get("/demo/api/ward")
def ward() -> dict:
    data = _api("/api/patients")
    _track_milestones(data["patients"])
    metrics_json = _safe(lambda: _api("/api/metrics"), {})
    return {"patients": data["patients"], "metrics": metrics_json}


@app.get("/demo/api/patient/{patient_id}")
def patient(patient_id: str) -> dict:
    if not PATIENT.match(patient_id):
        raise HTTPException(422, "patient id looks like P007")
    return _api(f"/api/patients/{patient_id}")


@app.get("/demo/api/alerts")
def alerts(minutes: int = 15) -> dict:
    return _api("/api/alerts", since_minutes=max(1, min(minutes, 1440)), limit=40)


@app.get("/demo/api/batch")
def batch_view(sim_day: int | None = None) -> dict:
    day = sim_day if sim_day is not None else _safe(queries.latest_risk_day)
    return {
        "sim_day": day,
        "daily_risk": _safe(lambda: queries.daily_risk(day), []) if day is not None else [],
        "lab_days": _safe(queries.lab_days, []),
        "health": _safe(queries.health_events, []),
        "dag_runs": _safe(airflow.recent_runs, None),
        "reports": _safe(lambda: batch.list_reports(REPORTS), []),
    }


# ---------------------------------------------------------------- controls --

SCENARIO_TEXT = {
    "none": "Normal ward",
    "spike": "Sudden deterioration",
    "hr_spike": "Heart-rate spike",
    "spo2_drop": "Gradual oxygen drop",
    "outage": "Sensor feed outage",
}


@app.post("/demo/api/scenario")
def start_scenario(scenario: str = Body(...), patient: str = Body("P007"),
                   duration_s: float = Body(180.0)) -> dict:
    if not PATIENT.match(patient):
        raise HTTPException(422, "patient id looks like P007")
    if not 20 <= duration_s <= 900:
        raise HTTPException(422, "duration must be 20-900 s")
    try:
        feed.set_scenario(scenario, patient, duration_s)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    who = "" if scenario in ("none", "outage") else f" for {patient}"
    _log_action(f"{SCENARIO_TEXT.get(scenario, scenario)}{who}",
                "" if scenario == "none" else f"starts in {feed.lead_s:.0f} s, lasts {duration_s:.0f} s")
    return feed.status()


@app.post("/demo/api/malformed")
def malformed(rate: float = Body(..., embed=True)) -> dict:
    try:
        feed.set_malformed_rate(rate)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    _log_action("Malformed readings " + ("on" if rate else "off"),
                f"{rate:.0%} of readings broken on purpose" if rate else "")
    return feed.status()


@app.post("/demo/api/feed/{action}")
def feed_control(action: str) -> dict:
    if action == "pause":
        feed.pause()
        _log_action("Feed paused", "no readings reach Kafka")
    elif action == "resume":
        feed.resume()
        _log_action("Feed resumed")
    else:
        raise HTTPException(404)
    return feed.status()


@app.post("/demo/api/dag/run")
def run_dag(sim_day: int | None = Body(None, embed=True)) -> dict:
    day = sim_day if sim_day is not None else _sim_clock()["last_complete_day"]
    if day < 0:
        raise HTTPException(422, "sim_day must be >= 0")
    try:
        run = airflow.trigger(day)
    except requests.RequestException as exc:
        raise HTTPException(502, f"Airflow did not accept the run: {exc}") from exc
    _log_action(f"Daily consolidation triggered for day {day}", run.get("dag_run_id", ""))
    return {"sim_day": day, "dag_run_id": run.get("dag_run_id"), "state": run.get("state")}


@app.post("/demo/api/labs/corrupt")
def corrupt_lab_file(sim_day: int = Body(..., embed=True)) -> dict:
    from simulators.lab_batch.main import build_simulator

    rows = build_simulator().rows_for_day(sim_day)
    info = batch.write_corrupt_file(LANDING, sim_day, rows)
    _log_action(f"Corrupted lab file written for day {sim_day}", "; ".join(info["faults"]))
    return info


@app.post("/demo/api/labs/restore")
def restore_lab_file(sim_day: int = Body(..., embed=True)) -> dict:
    from simulators.lab_batch.main import build_simulator, write_day

    path = write_day(build_simulator(), Path(LANDING), sim_day, force=True)
    _log_action(f"Correct lab file restored for day {sim_day}", str(path))
    return {"sim_day": sim_day, "path": str(path)}
