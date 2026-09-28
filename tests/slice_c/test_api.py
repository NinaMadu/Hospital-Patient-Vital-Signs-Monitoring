"""API endpoints with FastAPI's TestClient and a fake repository (no Postgres)."""
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from api import deps  # noqa: E402
from api.db import DatabaseUnavailable  # noqa: E402
from api.main import app  # noqa: E402

NOW = datetime(2026, 9, 28, 8, 0, tzinfo=timezone.utc)


def status_row(pid="P001", **over) -> dict:
    row = {
        "patient_id": pid, "sim_day": 96,
        "window_start": NOW - timedelta(minutes=2), "window_end": NOW,
        "last_event_time": NOW, "seconds_since_last_reading": 3.0,
        "avg_heart_rate": 78.0, "min_heart_rate": 70.0, "max_heart_rate": 85.0,
        "avg_spo2": 97.0, "min_spo2": 96.0, "max_spo2": 98.0,
        "avg_systolic_bp": 120.0, "min_systolic_bp": 112.0, "max_systolic_bp": 128.0,
        "avg_diastolic_bp": 78.0, "min_diastolic_bp": 72.0, "max_diastolic_bp": 84.0,
        "avg_temperature": 36.8, "min_temperature": 36.6, "max_temperature": 37.0,
        "reading_count": 30, "abnormal_count": 0, "hr_trend": None, "spo2_trend": None,
        "vital_risk_score": None, "vital_risk_category": None, "updated_at": NOW,
        "alerts_last_10m": 0, "critical_alerts_last_10m": 0,
    }
    row.update(over)
    return row


# P007 is deteriorating: low SpO2 (+2) and high average heart rate (+1) -> 3, WATCH.
SICK = status_row("P007", avg_heart_rate=140.0, max_heart_rate=158.0, min_spo2=86.0,
                  abnormal_count=25, alerts_last_10m=40, critical_alerts_last_10m=2)
LAB = {"sim_day": 95, "test_type": "crp", "result_value": 90.0, "unit": "mg/L",
       "reference_low": 0.0, "reference_high": 5.0, "collected_at": NOW}
ALERT = {"alert_id": "P007:SUSTAINED_SPO2_LOW:1790000000", "patient_id": "P007",
         "severity": "CRITICAL", "rule": "SUSTAINED_SPO2_LOW", "event_time": NOW}


class FakeRepo:
    def __init__(self, statuses=(), daily_risk=None, labs=(), alerts=(), down=False):
        self.statuses, self.daily_risk = list(statuses), daily_risk
        self.labs, self.alert_rows, self.down = list(labs), list(alerts), down
        self.alert_calls: list[dict] = []

    def _check(self):
        if self.down:
            raise DatabaseUnavailable("connection refused")

    def ping(self):
        self._check()

    def current_status(self, patient_id=None):
        self._check()
        return [r for r in self.statuses if patient_id in (None, r["patient_id"])]

    def alerts(self, **kw):
        self._check()
        self.alert_calls.append(kw)
        return [a for a in self.alert_rows if kw.get("patient_id") in (None, a["patient_id"])]

    def alert_counts(self, since_minutes=60):
        return [{"severity": "CRITICAL", "alert_type": "SUSTAINED", "alerts": 2}]

    def latest_daily_risk(self, patient_id):
        return self.daily_risk

    def latest_labs(self, patient_id):
        return self.labs

    def freshness(self):
        self._check()
        return {"patients": len(self.statuses), "status_age_seconds": 2.0,
                "last_reading_age_seconds": 3.0, "alerts_total": 5, "last_alert_age_seconds": 9.0,
                "latest_lab_sim_day": 95, "latest_risk_sim_day": 95, "recent_failures": 0}


@pytest.fixture
def use(monkeypatch):
    """use(FakeRepo(...)) -> a TestClient whose endpoints and /metrics see that repo."""
    def _use(repo):
        app.dependency_overrides[deps.get_repo] = lambda: repo
        monkeypatch.setattr(deps, "get_repo", lambda: repo)      # for the /metrics collector
        return TestClient(app)
    yield _use
    app.dependency_overrides.clear()


# ------------------------------------------------------------------ health --

def test_health_ok(use):
    r = use(FakeRepo()).get("/health")
    assert r.status_code == 200 and r.json() == {"status": "ok", "postgres": "ok"}


def test_health_reports_database_down_with_503(use):
    r = use(FakeRepo(down=True)).get("/health")
    assert r.status_code == 503 and r.json()["postgres"] == "unavailable"


def test_endpoints_answer_503_when_database_down(use):
    assert use(FakeRepo(down=True)).get("/api/patients").status_code == 503


# ---------------------------------------------------------------- patients --

def test_patients_list_scores_with_shared_risk_rules(use):
    body = use(FakeRepo([status_row(), SICK])).get("/api/patients").json()
    assert body["count"] == 2
    p1, p7 = body["patients"]
    assert p1["vital_risk"] == {"score": 0, "category": "NORMAL", "reasons": [],
                                "source": "computed_by_api"}
    assert p7["vital_risk"]["score"] == 3 and p7["vital_risk"]["category"] == "WATCH"
    assert set(p7["vital_risk"]["reasons"]) == {"spo2_low", "heart_rate_high"}
    assert p7["vitals"]["spo2"]["min"] == 86.0 and p7["critical_alerts_last_10m"] == 2


def test_patients_sort_by_risk_and_filter(use):
    client = use(FakeRepo([status_row(), SICK]))
    assert client.get("/api/patients?sort=risk").json()["patients"][0]["patient_id"] == "P007"
    watch = client.get("/api/patients?category=WATCH").json()
    assert [p["patient_id"] for p in watch["patients"]] == ["P007"]
    assert client.get("/api/patients?category=BAD").status_code == 422


def test_score_from_q1_is_used_when_present(use):
    row = status_row(vital_risk_score=4, vital_risk_category="CONCERNING")
    risk = use(FakeRepo([row])).get("/api/patients").json()["patients"][0]["vital_risk"]
    assert risk["score"] == 4 and risk["source"] == "patient_current_status"


def test_stale_patient_is_flagged(use):
    row = status_row(seconds_since_last_reading=120.0)
    assert use(FakeRepo([row])).get("/api/patients").json()["patients"][0]["stale"] is True


# ------------------------------------------------------------- merged view --

def test_merged_view_combines_speed_and_batch_layers(use):
    daily = {"sim_day": 95, "patient_id": "P007", "lab_risk_score": 2, "lab_status": "AVAILABLE"}
    body = use(FakeRepo([SICK], daily_risk=daily, labs=[LAB], alerts=[ALERT])).get(
        "/api/patients/P007").json()
    c = body["combined"]
    assert (c["vital_risk_score"], c["lab_risk_score"], c["total_risk_score"]) == (3, 2, 5)
    assert c["risk_category"] == "CONCERNING" and c["lab_status"] == "AVAILABLE"
    assert c["vital_status"] == "LIVE"
    assert body["batch_view"]["labs"]["source"] == "daily_patient_risk"
    assert body["batch_view"]["labs"]["abnormal"] == ["crp:HIGH"]
    assert body["recent_alerts"][0]["rule"] == "SUSTAINED_SPO2_LOW"


def test_merged_view_without_labs_is_lab_unavailable_not_zero(use):
    c = use(FakeRepo([SICK])).get("/api/patients/P007").json()["combined"]
    assert c["lab_risk_score"] is None and c["lab_status"] == "LAB_UNAVAILABLE"
    assert c["total_risk_score"] == 3 and c["risk_category"] == "WATCH"


def test_merged_view_uses_newer_lab_results_before_the_risk_join_runs(use):
    old_daily = {"sim_day": 94, "lab_risk_score": 0, "lab_status": "AVAILABLE"}
    labs = use(FakeRepo([SICK], daily_risk=old_daily, labs=[LAB])).get(
        "/api/patients/P007").json()["batch_view"]["labs"]
    # CRP 90 > 5 * 1.5 -> far out of range -> 2 points, computed with the shared rules.
    assert labs["source"] == "lab_results" and labs["sim_day"] == 95 and labs["score"] == 2


def test_unknown_patient_404_and_bad_id_422(use):
    client = use(FakeRepo())
    assert client.get("/api/patients/P999").status_code == 404
    assert client.get("/api/patients/X1").status_code == 422


# ------------------------------------------------------------------ alerts --

def test_alerts_filters_are_passed_to_the_repository(use):
    repo = FakeRepo(alerts=[ALERT])
    body = use(repo).get("/api/alerts?patient_id=P007&severity=CRITICAL&since_minutes=30&limit=5").json()
    assert body["count"] == 1
    assert repo.alert_calls[-1] == {"patient_id": "P007", "severity": "CRITICAL", "alert_type": None,
                                    "rule": None, "since_minutes": 30, "limit": 5}


@pytest.mark.parametrize("query", ["limit=0", "limit=501", "severity=LOW", "patient_id=7",
                                   "since_minutes=0", "rule=spo2;drop"])
def test_alerts_rejects_bad_parameters(use, query):
    assert use(FakeRepo()).get(f"/api/alerts?{query}").status_code == 422


# ----------------------------------------------------------------- metrics --

def test_prometheus_metrics_include_requests_and_serving_health(use):
    client = use(FakeRepo([SICK]))
    client.get("/api/patients")
    text = client.get("/metrics").text
    assert 'ward_api_requests_total{method="GET",route="/api/patients",status="200"}' in text
    assert "ward_postgres_up 1.0" in text
    assert 'ward_patients_by_vital_category{category="WATCH"} 1.0' in text
    assert "ward_lab_latest_sim_day 95.0" in text


def test_prometheus_metrics_when_database_down(use):
    assert "ward_postgres_up 0.0" in use(FakeRepo(down=True)).get("/metrics").text


def test_json_metrics_summary(use):
    body = use(FakeRepo([status_row(), SICK])).get("/api/metrics").json()
    assert body["patients_by_vital_category"] == {"NORMAL": 1, "WATCH": 1, "CONCERNING": 0}
    assert body["serving_store"]["latest_lab_sim_day"] == 95
    assert isinstance(body["current_sim_day"], int) and isinstance(body["api_requests"], list)
