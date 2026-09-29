"""Demo control centre: the switchable feed, the bad-lab-file drill and the HTTP endpoints.

No Kafka, Postgres or Airflow needed: the Kafka producer is a fake that acknowledges at once.
"""
from __future__ import annotations

import json
import time
from datetime import timedelta
from pathlib import Path

import pytest

from common import lab_feed, sim_clock
from common.config import get_settings
from demo.control import batch
from demo.control.producer import SUPPORTED, ScenarioProducer, looks_malformed
from simulators.lab_batch.main import build_simulator, write_day


class FakeMsg:
    def __init__(self, key, value, partition):
        self._key, self._value, self._partition = key, value, partition

    def key(self):
        return self._key.encode()

    def value(self):
        return self._value

    def partition(self):
        return self._partition


class FakeKafka:
    """Acknowledges every message on the next poll(), like a healthy broker."""

    def __init__(self):
        self.pending, self.messages = [], []

    def produce(self, topic, key, value, on_delivery):
        msg = FakeMsg(key, value, int(key[1:]) % 3)
        self.messages.append((topic, key, value))
        self.pending.append((on_delivery, msg))

    def poll(self, _timeout=0):
        while self.pending:
            cb, msg = self.pending.pop(0)
            cb(None, msg)

    def flush(self, _timeout=0):
        self.poll()
        return 0


def wait_for(cond, timeout=8.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return True
        time.sleep(0.05)
    return False


@pytest.fixture
def feed():
    kafka = FakeKafka()
    f = ScenarioProducer(topic="patient-vitals", producer_factory=lambda: kafka, lead_s=0.5)
    f.kafka = kafka
    yield f
    f.close()


# ------------------------------------------------------------------ feed --

def test_feed_streams_valid_readings_keyed_by_patient(feed):
    feed.start()
    assert wait_for(lambda: feed.counters.sent >= 5)
    topic, key, value = feed.kafka.messages[0]
    assert topic == "patient-vitals"
    assert json.loads(value)["patient_id"] == key
    status = feed.status()
    assert status["running"] and not status["paused"]
    assert status["malformed_sent"] == 0
    assert sum(status["per_partition"].values()) == status["sent"]


def test_scenario_switch_changes_the_feed_on_its_own_timeline(feed):
    feed.start()
    assert wait_for(lambda: feed.counters.sent >= 1)
    feed.set_scenario("spike", "P003", duration_s=60)
    run = feed.status()["scenario"]
    assert run["scenario"] == "spike" and run["patient"] == "P003"
    assert run["ends_at"] - run["starts_at"] == pytest.approx(60)
    # Once the scenario is active, P003's readings are the spike values.
    assert wait_for(lambda: feed.status()["scenario"]["phase"] == "active")
    n_before = len(feed.kafka.messages)
    assert wait_for(lambda: any(k == "P003" for _, k, _ in feed.kafka.messages[n_before:]))
    p003 = [json.loads(v) for _, k, v in feed.kafka.messages[n_before:] if k == "P003"]
    assert all(r["heart_rate"] >= 135 and r["spo2"] <= 89 for r in p003)


def test_back_to_normal_clears_the_scenario(feed):
    feed.set_scenario("spike", "P007", duration_s=60)
    feed.set_scenario("none")
    assert feed.status()["scenario"] is None
    assert feed.status()["running"]


def test_unknown_scenario_is_refused(feed):
    with pytest.raises(ValueError):
        feed.set_scenario("meteor_strike")
    assert "spike" in SUPPORTED


def test_malformed_rate_sends_broken_readings(feed):
    feed.set_malformed_rate(0.5)
    feed.start()
    assert wait_for(lambda: feed.counters.malformed >= 2)
    with pytest.raises(ValueError):
        feed.set_malformed_rate(0.9)


def test_pause_stops_the_feed_and_resume_restarts_it(feed):
    feed.start()
    assert wait_for(lambda: feed.counters.sent >= 1)
    feed.pause()
    assert not feed.status()["running"]
    sent = feed.counters.sent
    time.sleep(0.6)
    assert feed.counters.sent == sent
    feed.resume()
    assert wait_for(lambda: feed.counters.sent > sent)


@pytest.mark.parametrize("payload, bad", [
    ('{"heart_rate": 80, "spo2": 97.1, "systolic_bp": 120, "diastolic_bp": 80, "temperature": 36.8}', False),
    ("{not valid json", True),
    ('{"spo2": 97.1, "systolic_bp": 120, "diastolic_bp": 80, "temperature": 36.8}', True),
    ('{"heart_rate": "fast", "spo2": 97.1, "systolic_bp": 120, "diastolic_bp": 80, "temperature": 36.8}', True),
    ('{"heart_rate": 80, "spo2": 140.0, "systolic_bp": 120, "diastolic_bp": 80, "temperature": 36.8}', True),
    ('{"heart_rate": 80, "spo2": null, "systolic_bp": 120, "diastolic_bp": 80, "temperature": 36.8}', True),
])
def test_looks_malformed(payload, bad):
    assert looks_malformed(payload) is bad


# ------------------------------------------------------- lab file drill --

def _validate(landing: Path, day: int):
    s = get_settings()
    return lab_feed.validate_file(landing, day, s.lab_simulator.tests, s.patients.id_prefix,
                                  sim_clock.day_start(day), sim_clock.day_end(day))


def test_corrupt_file_is_rejected_and_restore_makes_it_valid(tmp_path):
    day = 12
    sim = build_simulator()
    write_day(sim, tmp_path, day)
    assert _validate(tmp_path, day).ok

    info = batch.write_corrupt_file(tmp_path, day, sim.rows_for_day(day))
    result = _validate(tmp_path, day)
    assert not result.ok
    assert result.error_count >= 3, result.errors
    assert len(info["faults"]) == 3
    # The marker still promises the full row count, so only content checks catch it.
    marker = json.loads(lab_feed.marker_path(tmp_path, day).read_text())
    assert marker["rows"] == len(sim.rows_for_day(day))

    write_day(sim, tmp_path, day, force=True)
    assert _validate(tmp_path, day).ok


def test_list_reports_newest_first(tmp_path):
    for d in (7, 9, 8):
        (tmp_path / f"risk_report_day={d}.html").write_text("x")
        (tmp_path / f"risk_report_day={d}.csv").write_text("x")
    (tmp_path / "notes.txt").write_text("x")
    reports = batch.list_reports(tmp_path, limit=2)
    assert [r["sim_day"] for r in reports] == [9, 8]
    assert reports[0]["files"] == ["risk_report_day=9.csv", "risk_report_day=9.html"]


class FakeDb:
    def __init__(self, rows):
        self.rows = rows

    def query(self, _sql, _params=None):
        return [dict(r) for r in self.rows]


def test_daily_risk_marks_patients_whose_labs_changed_the_category():
    q = batch.BatchQueries(FakeDb([
        {"patient_id": "P003", "vital_risk_score": 3, "total_risk_score": 4, "risk_category": "CONCERNING"},
        {"patient_id": "P012", "vital_risk_score": 3, "total_risk_score": 3, "risk_category": "WATCH"},
    ]))
    rows = {r["patient_id"]: r for r in q.daily_risk(85)}
    assert rows["P003"]["vitals_only_category"] == "WATCH"
    assert rows["P003"]["labs_changed_category"] is True
    assert rows["P012"]["labs_changed_category"] is False


# ------------------------------------------------------------------ http --

@pytest.fixture
def client(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    from demo.control import server

    kafka = FakeKafka()
    feed = ScenarioProducer(topic="patient-vitals", producer_factory=lambda: kafka, lead_s=0.5)
    monkeypatch.setattr(server, "feed", feed)
    monkeypatch.setattr(server, "REPORTS", str(tmp_path))
    monkeypatch.setattr(server, "LANDING", str(tmp_path / "landing"))
    triggered = []

    class FakeAirflow:
        def trigger(self, day):
            triggered.append(day)
            return {"dag_run_id": f"demo__day{day}", "state": "queued"}

        def healthy(self):
            return True

        def recent_runs(self):
            return []

    monkeypatch.setattr(server, "airflow", FakeAirflow())
    (tmp_path / "risk_report_day=5.html").write_text("<h1>day 5</h1>")
    c = TestClient(server.app)          # no `with`: the lifespan (metrics port, autostart) is skipped
    c.triggered = triggered
    yield c
    feed.close()


def test_page_and_static_files_are_served(client):
    assert "Ward board" in client.get("/").text
    assert client.get("/static/app.js").status_code == 200


def test_report_files_only_by_name_pattern(client):
    assert client.get("/reports/risk_report_day=5.html").text == "<h1>day 5</h1>"
    assert client.get("/reports/..%2F.env").status_code == 404
    assert client.get("/reports/risk_report_day=6.html").status_code == 404


def test_scenario_endpoint_validates_and_logs_the_action(client):
    from demo.control import server

    assert client.post("/demo/api/scenario", json={"scenario": "spike", "patient": "X1"}).status_code == 422
    assert client.post("/demo/api/scenario", json={"scenario": "spike", "duration_s": 5}).status_code == 422
    assert client.post("/demo/api/scenario", json={"scenario": "nope"}).status_code == 422
    r = client.post("/demo/api/scenario", json={"scenario": "spike", "patient": "P004", "duration_s": 60})
    assert r.status_code == 200
    assert r.json()["scenario"]["patient"] == "P004"
    assert server.actions[-1]["title"] == "Sudden deterioration for P004"


def test_dag_run_defaults_to_the_last_complete_day(client):
    from demo.control import server

    r = client.post("/demo/api/dag/run", json={})
    assert r.status_code == 200
    assert client.triggered == [server._sim_clock()["last_complete_day"]]
    client.post("/demo/api/dag/run", json={"sim_day": 3})
    assert client.triggered[-1] == 3


def test_lab_drill_endpoints_write_to_the_landing_folder(client, tmp_path):
    day = 4
    r = client.post("/demo/api/labs/corrupt", json={"sim_day": day})
    assert r.status_code == 200
    assert not _validate(tmp_path / "landing", day).ok
    assert client.post("/demo/api/labs/restore", json={"sim_day": day}).status_code == 200
    assert _validate(tmp_path / "landing", day).ok


def test_milestones_record_time_to_concerning():
    from demo.control import server

    feed = ScenarioProducer(topic="t", producer_factory=FakeKafka, lead_s=0)
    feed.set_scenario("spike", "P007", 60)
    feed.run.starts_at -= 12                     # the scenario began 12 s ago
    server_feed, server.feed = server.feed, feed
    try:
        server.queries.first_alert_since = lambda pid, since: since + timedelta(seconds=9)
        server._track_milestones([{"patient_id": "P007", "stale": False,
                                   "vital_risk": {"category": "CONCERNING"}}])
        assert feed.run.concerning_after_s == pytest.approx(12, abs=1)
        assert feed.run.first_alert_after_s == pytest.approx(9, abs=0.1)
    finally:
        server.feed = server_feed
        del server.queries.first_alert_since
        feed.close()


# ------------------------------------------------------------ live pipeline --

def test_spark_metrics_parses_the_listener_gauges(monkeypatch):
    from demo.control import pipeline

    text = "\n".join([
        "# HELP ward_streaming_batch_id last completed batch",
        'ward_streaming_batch_id{instance="",job="spark_streaming",query="q1_windows"} 5720',
        'ward_streaming_input_rows{instance="",job="spark_streaming",query="q1_windows"} 41',
        'ward_streaming_batch_duration_ms{instance="",job="spark_streaming",query="q2_archive"} 2366',
        'push_time_seconds{instance="",job="spark_streaming"} 1.79e+09',
    ])

    class Resp:
        def raise_for_status(self):
            pass

    Resp.text = text
    monkeypatch.setattr(pipeline.requests, "get", lambda url, timeout: Resp())
    m = pipeline.spark_metrics("http://pushgateway:9091")
    assert m == {"q1_windows": {"batch_id": 5720.0, "input_rows": 41.0},
                 "q2_archive": {"batch_duration_ms": 2366.0}}


def test_lake_and_landing_stats(tmp_path):
    from demo.control import pipeline

    day = tmp_path / "lake" / "sim_day=7"
    day.mkdir(parents=True)
    (day / "part-0.parquet").write_bytes(b"x" * 100)
    (day / "part-1.parquet").write_bytes(b"x" * 50)
    (tmp_path / "lake" / "sim_day=6").mkdir()
    assert pipeline.lake_stats(tmp_path / "lake", 7) == {"sim_day": 7, "files": 2, "bytes": 150, "days": 2}
    assert pipeline.lake_stats(tmp_path / "missing", 7) is None

    landing = tmp_path / "landing"
    write_day(build_simulator(), landing, 3)
    write_day(build_simulator(), landing, 4)
    info = pipeline.landing_stats(landing)
    assert info["latest_day"] == 4 and info["files"] == 2 and info["marker"] is True
    assert info["patients"] > 0 and info["corrupted"] is False
    batch.write_corrupt_file(landing, 4, build_simulator().rows_for_day(4))
    assert pipeline.landing_stats(landing)["corrupted"] is True


def test_live_sources_never_wait_for_a_slow_or_background_source():
    from demo.control import pipeline

    def slow():
        time.sleep(1.5)
        return "late"

    def broken():
        raise RuntimeError("down")

    src = pipeline.LiveSources({"fast": lambda: 1, "slow": slow, "bg": slow, "broken": broken},
                               wait_s=0.3, background=frozenset({"bg"}))
    t = time.monotonic()
    first = src.snapshot()
    assert time.monotonic() - t < 1.0            # the answer did not wait for "slow"
    assert first["fast"] == 1 and first["slow"] is None and first["broken"] is None
    assert wait_for(lambda: src.snapshot()["slow"] == "late", timeout=4)
    assert src.snapshot()["bg"] == "late"         # background sources catch up on their own


def test_pipeline_endpoint_returns_every_stage(client, monkeypatch):
    from demo.control import pipeline, server

    fake = pipeline.LiveSources({"kafka": lambda: {"patient-vitals": {"partitions": {0: 5}, "total": 5}},
                                 "db": lambda: None})
    monkeypatch.setattr(server, "sources", fake)
    body = client.get("/demo/api/pipeline").json()
    assert {"now", "clock", "feed", "kafka", "db"} <= set(body)
    assert body["kafka"]["patient-vitals"]["total"] == 5
    assert body["feed"]["supported_scenarios"]
    assert "Live pipeline" in client.get("/").text
    assert client.get("/static/pipeline.js").status_code == 200
