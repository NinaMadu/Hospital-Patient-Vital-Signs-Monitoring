"""Every SQL query the API runs, in one place.

Owner: Member C.
Routers never write SQL; they call these methods. Tests replace the whole repository with
a fake (see tests/slice_c/test_api.py), so the endpoints are tested without Postgres.

Tables read (all written by other components):
  patient_current_status  speed view, Spark Q1 (Member A)
  vital_alerts            Spark Q3 -> patient-alerts -> alert consumer (Member C)
  daily_patient_risk      batch view, risk consolidation job (Member B)
  lab_results             daily lab feed, Airflow DAG (Member B)
  pipeline_health         DAG failure callbacks and checks
"""
from __future__ import annotations

from api.db import Database

ALERT_FIELDS = """alert_id, patient_id, bed_id, alert_type, rule, severity, metric,
    metric_value, threshold, event_time, window_start, window_end, abnormal_count,
    reading_count, sim_day, message, detected_at, created_at"""


class WardRepository:
    def __init__(self, db: Database):
        self.db = db

    def ping(self) -> None:
        self.db.ping()

    # ------------------------------------------------------------ speed view --

    def current_status(self, patient_id: str | None = None) -> list[dict]:
        """patient_current_status rows plus each patient's alert counts in the last 10 min."""
        where, params = ("WHERE s.patient_id = %s", (patient_id,)) if patient_id else ("", ())
        return self.db.query(f"""
            SELECT s.*,
                   COALESCE(a.alerts, 0)   AS alerts_last_10m,
                   COALESCE(a.critical, 0) AS critical_alerts_last_10m,
                   EXTRACT(EPOCH FROM now() - s.last_event_time)::float AS seconds_since_last_reading
            FROM patient_current_status s
            LEFT JOIN (
                SELECT patient_id, count(*) AS alerts,
                       count(*) FILTER (WHERE severity = 'CRITICAL') AS critical
                FROM vital_alerts
                WHERE event_time > now() - interval '10 minutes'
                GROUP BY patient_id
            ) a USING (patient_id)
            {where}
            ORDER BY s.patient_id""", params)

    # ---------------------------------------------------------------- alerts --

    def alerts(self, patient_id: str | None = None, severity: str | None = None,
               alert_type: str | None = None, rule: str | None = None,
               since_minutes: int = 60, limit: int = 50) -> list[dict]:
        conditions = ["event_time > now() - make_interval(mins => %s)"]
        params: list = [since_minutes]
        for column, value in (("patient_id", patient_id), ("severity", severity),
                              ("alert_type", alert_type), ("rule", rule)):
            if value:
                conditions.append(f"{column} = %s")
                params.append(value)
        params.append(limit)
        return self.db.query(f"""
            SELECT {ALERT_FIELDS} FROM vital_alerts
            WHERE {" AND ".join(conditions)}
            ORDER BY event_time DESC, alert_id
            LIMIT %s""", params)

    def alert_counts(self, since_minutes: int = 60) -> list[dict]:
        return self.db.query("""
            SELECT severity, alert_type, count(*) AS alerts FROM vital_alerts
            WHERE event_time > now() - make_interval(mins => %s)
            GROUP BY severity, alert_type ORDER BY severity, alert_type""", (since_minutes,))

    # ------------------------------------------------------------ batch view --

    def latest_daily_risk(self, patient_id: str) -> dict | None:
        """The patient's newest row in the consolidated daily risk table (batch layer)."""
        rows = self.db.query("""
            SELECT * FROM daily_patient_risk WHERE patient_id = %s
            ORDER BY sim_day DESC LIMIT 1""", (patient_id,))
        return rows[0] if rows else None

    def latest_labs(self, patient_id: str) -> list[dict]:
        """The patient's lab results from the newest sim_day that has any."""
        return self.db.query("""
            SELECT sim_day, test_type, result_value, unit, reference_low, reference_high,
                   collected_at
            FROM lab_results
            WHERE patient_id = %s
              AND sim_day = (SELECT max(sim_day) FROM lab_results WHERE patient_id = %s)
            ORDER BY test_type, collected_at DESC""", (patient_id, patient_id))

    # ------------------------------------------------------------ freshness --

    def freshness(self) -> dict:
        """Row counts and ages of the serving tables (for /api/metrics and Prometheus)."""
        return self.db.query("""
            SELECT
              (SELECT count(*) FROM patient_current_status)                 AS patients,
              (SELECT EXTRACT(EPOCH FROM now() - max(updated_at))::float
                 FROM patient_current_status)                                AS status_age_seconds,
              (SELECT EXTRACT(EPOCH FROM now() - max(last_event_time))::float
                 FROM patient_current_status)                                AS last_reading_age_seconds,
              (SELECT count(*) FROM vital_alerts)                            AS alerts_total,
              (SELECT EXTRACT(EPOCH FROM now() - max(created_at))::float
                 FROM vital_alerts)                                          AS last_alert_age_seconds,
              (SELECT max(sim_day) FROM lab_results)                         AS latest_lab_sim_day,
              (SELECT max(sim_day) FROM daily_patient_risk)                  AS latest_risk_sim_day,
              (SELECT count(*) FROM pipeline_health
                 WHERE status = 'FAIL' AND checked_at > now() - interval '30 minutes')
                                                                             AS recent_failures
            """)[0]
