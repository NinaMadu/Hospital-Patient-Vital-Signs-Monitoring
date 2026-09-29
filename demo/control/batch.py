"""Batch-layer helpers for the demo: Airflow runs, the daily risk table, the lab-file drill.

Nothing here changes how the pipeline works. The demo triggers the real DAG through
Airflow's REST API and reads the tables the batch jobs write.
"""
from __future__ import annotations

import csv
import io
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from common import risk_rules as rr
from common.lab_feed import COLUMNS, file_path, marker_path

DAG_ID = "daily_lab_consolidation"
TASK_ORDER = ("resolve_sim_day", "wait_for_lab_file", "validate_lab_file", "load_lab_results",
              "wait_for_lake_settle", "vital_daily_summary", "risk_consolidation",
              "generate_report")
REPORT_NAME = re.compile(r"^risk_report_day=(\d+)\.(html|csv)$")


# ------------------------------------------------------------------ Airflow --

class AirflowClient:
    """The few Airflow REST calls the demo needs (basic auth, see docker-compose.demo.yml)."""

    def __init__(self, base_url: str, user: str, password: str, timeout: float = 5.0):
        self.base = base_url.rstrip("/") + "/api/v1"
        self.auth = (user, password)
        self.timeout = timeout

    def _call(self, method: str, path: str, **kwargs) -> dict:
        r = requests.request(method, self.base + path, auth=self.auth, timeout=self.timeout,
                             **kwargs)
        r.raise_for_status()
        return r.json()

    def trigger(self, sim_day: int) -> dict:
        run_id = f"demo__day{sim_day}__{datetime.now(timezone.utc):%H%M%S}"
        return self._call("POST", f"/dags/{DAG_ID}/dagRuns",
                          json={"dag_run_id": run_id, "conf": {"sim_day": sim_day}})

    def recent_runs(self, limit: int = 5) -> list[dict]:
        runs = self._call("GET", f"/dags/{DAG_ID}/dagRuns",
                          params={"order_by": "-start_date", "limit": limit})["dag_runs"]
        out = []
        for run in runs:
            tasks = self._call("GET", f"/dags/{DAG_ID}/dagRuns/{run['dag_run_id']}/taskInstances")
            states = {t["task_id"]: t["state"] for t in tasks["task_instances"]}
            out.append({
                "run_id": run["dag_run_id"],
                "run_type": run.get("run_type"),
                "state": run["state"],
                "sim_day": (run.get("conf") or {}).get("sim_day"),
                "start_date": run.get("start_date"),
                "end_date": run.get("end_date"),
                "tasks": [{"task_id": t, "state": states.get(t)} for t in TASK_ORDER],
            })
        return out

    def healthy(self) -> bool:
        try:
            base = self.base.removesuffix("/api/v1")
            r = requests.get(base + "/health", timeout=self.timeout)
            return r.ok and r.json().get("scheduler", {}).get("status") == "healthy"
        except (requests.RequestException, ValueError):
            return False


# --------------------------------------------------------------- database --

class BatchQueries:
    """Read-only queries on the batch tables (same Database class as the API)."""

    def __init__(self, db):
        self.db = db

    def latest_risk_day(self) -> int | None:
        return self.db.query("SELECT max(sim_day) AS d FROM daily_patient_risk")[0]["d"]

    def daily_risk(self, sim_day: int) -> list[dict]:
        rows = self.db.query("""
            SELECT patient_id, vital_risk_score, vital_reasons, lab_risk_score, lab_sim_day,
                   total_risk_score, risk_category, lab_status, abnormal_labs, generated_at
            FROM daily_patient_risk WHERE sim_day = %s
            ORDER BY total_risk_score DESC, patient_id""", (sim_day,))
        for r in rows:
            r["vitals_only_category"] = rr.category(r["vital_risk_score"] or 0)
            r["labs_changed_category"] = r["vitals_only_category"] != r["risk_category"]
        return rows

    def lab_days(self, limit: int = 6) -> list[dict]:
        return self.db.query("""
            SELECT sim_day, count(*) AS rows, count(DISTINCT patient_id) AS patients,
                   max(loaded_at) AS loaded_at
            FROM lab_results GROUP BY sim_day ORDER BY sim_day DESC LIMIT %s""", (limit,))

    def health_events(self, limit: int = 8) -> list[dict]:
        return self.db.query("""
            SELECT checked_at, component, check_name, status, value, details
            FROM pipeline_health ORDER BY checked_at DESC LIMIT %s""", (limit,))

    def first_alert_since(self, patient_id: str, since: datetime) -> datetime | None:
        rows = self.db.query("""
            SELECT min(created_at) AS first FROM vital_alerts
            WHERE patient_id = %s AND event_time >= %s""", (patient_id, since))
        return rows[0]["first"]


# ----------------------------------------------------------------- reports --

def list_reports(reports_dir: str | Path, limit: int = 6) -> list[dict]:
    days = {}
    for p in Path(reports_dir).glob("risk_report_day=*.*"):
        m = REPORT_NAME.match(p.name)
        if m:
            days.setdefault(int(m.group(1)), []).append(p.name)
    return [{"sim_day": d, "files": sorted(days[d])} for d in sorted(days, reverse=True)[:limit]]


# ------------------------------------------------------ bad lab file drill --

def _write(path: Path, text: str) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def write_corrupt_file(landing_dir: str | Path, sim_day: int, rows: list[dict]) -> dict:
    """Overwrite day N's lab file with a broken copy that the DAG must reject.

    Three faults a real feed can have: a result that is not a number, a reference range
    upside down, and a sample time outside the day. The marker keeps the true row count,
    so only the content checks catch it.
    """
    if len(rows) < 3:
        raise ValueError("need at least 3 rows to corrupt")
    bad = [dict(r) for r in rows]
    bad[0]["result_value"] = "not-a-number"
    bad[1]["reference_low"], bad[1]["reference_high"] = bad[1]["reference_high"], bad[1]["reference_low"]
    collected = bad[2]["collected_at"]
    if isinstance(collected, str):
        collected = datetime.fromisoformat(collected)
    bad[2]["collected_at"] = (collected + timedelta(days=2)).isoformat()

    landing = Path(landing_dir)
    landing.mkdir(parents=True, exist_ok=True)
    marker = marker_path(landing, sim_day)
    marker.unlink(missing_ok=True)
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(bad)
    _write(file_path(landing, sim_day), buf.getvalue())
    _write(marker, json.dumps({"sim_day": sim_day, "rows": len(bad),
                               "patients": len({r["patient_id"] for r in bad}),
                               "written_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                               "demo": "corrupted on purpose"}) + "\n")
    return {"sim_day": sim_day, "rows": len(bad),
            "faults": ["result_value is not a number (row 1)",
                       "reference_low > reference_high (row 2)",
                       "collected_at outside the sim day (row 3)"]}
