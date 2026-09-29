"""Live numbers for the demo's "Live pipeline" tab: one snapshot of every stage.

Each stage is read where it really lives, so the animation on the page is driven by the
pipeline itself, not by a script:

  Kafka        end offsets of patient-vitals (per partition), vitals-dlq, patient-alerts
  Spark        the listener's per-query gauges on the Pushgateway (batch id, rows/s, ...)
  Postgres     row counts and last-write times of the serving tables
  Lake         Parquet files Q2 wrote for the current sim day
  Landing      the newest lab file and its _SUCCESS marker
  Prometheus   alerts firing now (only with the monitoring profile)

A source that is down returns None; the page greys that stage out instead of failing.
"""
from __future__ import annotations

import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
from typing import Callable

import requests

from common.lab_feed import marker_path
from common.logger import get_logger

log = get_logger("demo-control")
LAB_FILE = re.compile(r"^labs_day=(\d+)\.csv$")
METRIC_LINE = re.compile(r'^ward_streaming_(\w+)\{[^}]*query="([^"]+)"[^}]*\}\s+(\S+)$')


class LiveSources:
    """Reads every source in parallel, and a slow source never holds up the answer.

    Each snapshot() starts a refresh of every source that is not already refreshing, then
    waits at most `wait_s`. A source still running (e.g. Prometheus when it is not started:
    the DNS lookup alone takes seconds) keeps its last value and updates it in the
    background, so the page's animation keeps its 2-second rhythm.
    """

    def __init__(self, jobs: dict[str, Callable], wait_s: float = 1.2,
                 background: frozenset[str] = frozenset()):
        self.jobs, self.wait_s = jobs, wait_s
        self.background = background   # slow sources whose last value is always good enough
        self.values: dict = {name: None for name in jobs}
        self._running: dict = {}
        self._failing: set[str] = set()
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=len(jobs) + 2, thread_name_prefix="pipeline-probe")

    def _refresh(self, name: str, fn: Callable) -> None:
        try:
            self.values[name] = fn()
            self._failing.discard(name)
        except Exception as exc:  # noqa: BLE001 - one broken source must not blank the page
            self.values[name] = None
            if name not in self._failing:          # log once per outage, not every poll
                self._failing.add(name)
                log.warning("pipeline_source_down", source=name, error=str(exc)[:200])

    def snapshot(self) -> dict:
        with self._lock:
            for name, fn in self.jobs.items():
                f = self._running.get(name)
                if f is None or f.done():
                    self._running[name] = self._pool.submit(self._refresh, name, fn)
            # a source that is known to be down is not waited for; it catches up when it's back
            futures = [f for name, f in self._running.items()
                       if name not in self._failing and name not in self.background]
        wait(futures, timeout=self.wait_s)
        return dict(self.values)


class KafkaOffsets:
    """End offsets per topic, read with a consumer that never joins a group or commits."""

    def __init__(self, bootstrap: str, topics: list[str]):
        self.bootstrap, self.topics = bootstrap, topics
        self._consumer = None
        self._partitions: dict[str, list[int]] = {}
        self._lock = threading.Lock()

    def _client(self):
        if self._consumer is None:
            from confluent_kafka import Consumer
            self._consumer = Consumer({"bootstrap.servers": self.bootstrap,
                                       "group.id": "demo-pipeline-view",
                                       "enable.auto.commit": False})
        return self._consumer

    def read(self) -> dict:
        from confluent_kafka import TopicPartition

        with self._lock:
            c = self._client()
            out = {}
            for topic in self.topics:
                if topic not in self._partitions:
                    meta = c.list_topics(topic, timeout=2.0).topics.get(topic)
                    if meta is None or meta.error is not None:
                        continue
                    self._partitions[topic] = sorted(meta.partitions)
                ends = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=2.0)[1]
                        for p in self._partitions[topic]}
                out[topic] = {"partitions": ends, "total": sum(ends.values())}
            return out


def spark_metrics(pushgateway_url: str) -> dict:
    """{query: {metric: value}} from the gauges spark/streaming/listener.py pushes."""
    r = requests.get(pushgateway_url.rstrip("/") + "/metrics", timeout=2)
    r.raise_for_status()
    out: dict[str, dict] = {}
    for line in r.text.splitlines():
        m = METRIC_LINE.match(line)
        if m:
            out.setdefault(m.group(2), {})[m.group(1)] = float(m.group(3))
    return out


def firing_alerts(prometheus_url: str) -> list[str]:
    r = requests.get(prometheus_url.rstrip("/") + "/api/v1/alerts", timeout=2)
    r.raise_for_status()
    return sorted({a["labels"].get("alertname", "?") for a in r.json()["data"]["alerts"]
                   if a.get("state") == "firing"})


def lake_stats(lake_dir: str | Path, sim_day: int) -> dict | None:
    root = Path(lake_dir)
    if not root.is_dir():
        return None
    files = list((root / f"sim_day={sim_day}").glob("*.parquet"))
    return {"sim_day": sim_day,
            "files": len(files),
            "bytes": sum(f.stat().st_size for f in files),
            "days": sum(1 for d in root.glob("sim_day=*") if d.is_dir())}


def landing_stats(landing_dir: str | Path) -> dict | None:
    root = Path(landing_dir)
    if not root.is_dir():
        return None
    days = sorted(int(m.group(1)) for p in root.iterdir() if (m := LAB_FILE.match(p.name)))
    if not days:
        return {"latest_day": None, "files": 0}
    day = days[-1]
    marker = marker_path(root, day)
    info = {}
    if marker.is_file():
        try:
            info = json.loads(marker.read_text(encoding="utf-8"))
        except ValueError:
            info = {}
    return {"latest_day": day, "files": len(days), "marker": marker.is_file(),
            "rows": info.get("rows"), "patients": info.get("patients"),
            "corrupted": bool(info.get("demo"))}


def db_stats(db) -> dict:
    """Row counts and freshness of the serving tables, in one round trip each."""
    status = db.query("""
        SELECT count(*) AS patients,
               count(*) FILTER (WHERE vital_risk_category = 'NORMAL') AS normal,
               count(*) FILTER (WHERE vital_risk_category = 'WATCH') AS watch,
               count(*) FILTER (WHERE vital_risk_category = 'CONCERNING') AS concerning,
               extract(epoch FROM now() - max(updated_at)) AS age_s
        FROM patient_current_status""")[0]
    alerts = db.query("""
        SELECT count(*) AS total,
               count(*) FILTER (WHERE created_at > now() - interval '1 minute') AS last_minute,
               extract(epoch FROM now() - max(created_at)) AS age_s
        FROM vital_alerts""")[0]
    latest_alerts = db.query("""
        SELECT alert_id, patient_id, rule, severity, metric, metric_value, created_at
        FROM vital_alerts ORDER BY created_at DESC LIMIT 10""")
    labs = db.query("""
        SELECT count(*) AS rows, max(sim_day) AS latest_day FROM lab_results""")[0]
    summary = db.query("""
        SELECT sim_day, count(*) AS rows FROM vital_daily_summary
        WHERE sim_day = (SELECT max(sim_day) FROM vital_daily_summary) GROUP BY sim_day""")
    risk = db.query("""
        SELECT sim_day, count(*) AS rows,
               count(*) FILTER (WHERE risk_category = 'CONCERNING') AS concerning,
               max(generated_at) AS generated_at
        FROM daily_patient_risk
        WHERE sim_day = (SELECT max(sim_day) FROM daily_patient_risk) GROUP BY sim_day""")
    return {"current_status": status, "alerts": alerts, "latest_alerts": latest_alerts,
            "labs": labs, "summary": summary[0] if summary else None,
            "risk": risk[0] if risk else None}
