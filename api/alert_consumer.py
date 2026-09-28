"""Consumes patient-alerts (consumer group) and stores alerts in vital_alerts.

Owner: Member C.

    docker compose up -d alert-consumer
    python -m api.alert_consumer            # local run (Kafka localhost:9094, Postgres localhost)

How it avoids losing or duplicating alerts:
  1. Auto-commit is off. The loop reads a batch, inserts it, and only then commits the
     Kafka offsets. If the process dies before the commit, the batch is read again.
  2. The insert is ON CONFLICT (alert_id) DO NOTHING and alert_id is deterministic, so a
     batch that is read twice (or an alert Spark sent twice) is stored once.
  3. If Postgres is down, the same batch is retried with a growing back-off and nothing is
     committed, so the alerts wait safely in Kafka until the database is back.
  (1) is at-least-once delivery; (1) + (2) together give effectively-once storage.

A message that is not a valid alert is logged, counted and skipped (committed), so one bad
message cannot block the partition forever.

Metrics are served on :8001/metrics (Prometheus job "alert-consumer"). The consumer lag of
group ward-alert-consumer is visible through kafka-exporter.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Iterable

from common import metrics
from common.config import get_settings
from common.logger import get_logger

GROUP_ID = os.getenv("ALERT_CONSUMER_GROUP", "ward-alert-consumer")
METRICS_PORT = int(os.getenv("ALERT_CONSUMER_METRICS_PORT", "8001"))
BATCH_SIZE = 200
POLL_TIMEOUT_S = 1.0

log = get_logger("alert-consumer")

# Columns written, in order. Must match the patient-alerts contract (q3_alerts.ALERT_COLUMNS).
COLUMNS = [
    "alert_id", "patient_id", "bed_id", "alert_type", "rule", "severity", "metric",
    "metric_value", "threshold", "event_time", "window_start", "window_end",
    "abnormal_count", "reading_count", "sim_day", "message", "detected_at",
]
REQUIRED = ("alert_id", "patient_id", "rule", "severity", "event_time", "sim_day")
TIMESTAMPS = ("event_time", "window_start", "window_end", "detected_at")
FLOATS = ("metric_value", "threshold")
INTS = ("abnormal_count", "reading_count", "sim_day")

INSERT_SQL = f"""
INSERT INTO vital_alerts ({", ".join(COLUMNS)}) VALUES %s
ON CONFLICT (alert_id) DO NOTHING
RETURNING alert_id, severity, alert_type
"""

# Brings a database created from the first draft schema up to date without `down -v`.
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS vital_alerts (
    alert_id TEXT PRIMARY KEY, patient_id TEXT NOT NULL, rule TEXT NOT NULL,
    severity TEXT NOT NULL, metric_value DOUBLE PRECISION, event_time TIMESTAMPTZ NOT NULL,
    sim_day INTEGER NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE vital_alerts
    ADD COLUMN IF NOT EXISTS bed_id TEXT,
    ADD COLUMN IF NOT EXISTS alert_type TEXT,
    ADD COLUMN IF NOT EXISTS metric TEXT,
    ADD COLUMN IF NOT EXISTS threshold DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS window_start TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS window_end TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS abnormal_count INTEGER,
    ADD COLUMN IF NOT EXISTS reading_count INTEGER,
    ADD COLUMN IF NOT EXISTS message TEXT,
    ADD COLUMN IF NOT EXISTS detected_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS ix_vital_alerts_patient_time ON vital_alerts (patient_id, event_time DESC);
CREATE INDEX IF NOT EXISTS ix_vital_alerts_time ON vital_alerts (event_time DESC);
"""

# ------------------------------------------------------------------ metrics --

CONSUMED = metrics.counter("alerts_consumed", "Alert messages read from Kafka")
STORED = metrics.counter("alerts_stored", "Alerts inserted into vital_alerts",
                         ["severity", "alert_type"])
# Create the known label sets up front so their series start at 0. A labelled series that
# first appears already at 3 has no earlier sample, so increase() stays 0 and the
# PatientCriticalAlert rule would miss the first critical alerts after a restart.
for _severity, _alert_type in (("WARNING", "THRESHOLD"), ("CRITICAL", "SUSTAINED")):
    STORED.labels(severity=_severity, alert_type=_alert_type)
DUPLICATES = metrics.counter("alerts_duplicate", "Alerts skipped because already stored")
INVALID = metrics.counter("alert_consumer_invalid_messages", "Messages that are not valid alerts")
DB_ERRORS = metrics.counter("alert_consumer_db_errors", "Failed attempts to store a batch")
LAST_STORED = metrics.gauge("alert_consumer_last_stored_timestamp_seconds",
                            "Unix time of the last successfully stored batch")
BATCH_SECONDS = metrics.histogram("alert_consumer_batch_seconds", "Time to store one batch",
                                  buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5))


# ------------------------------------------------------------------ parsing --

class InvalidAlert(ValueError):
    pass


def _timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).replace("Z", "+00:00")     # Spark writes ...Z; Python 3.10 wants +00:00
    ts = datetime.fromisoformat(text)
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def parse_alert(raw: bytes | str | None) -> tuple:
    """One Kafka message value -> a row tuple in COLUMNS order. Raises InvalidAlert."""
    if raw is None:
        raise InvalidAlert("empty message")
    try:
        data = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise InvalidAlert(f"not JSON: {exc}") from None
    if not isinstance(data, dict):
        raise InvalidAlert("not a JSON object")
    missing = [k for k in REQUIRED if data.get(k) in (None, "")]
    if missing:
        raise InvalidAlert(f"missing fields: {','.join(missing)}")
    try:
        row = dict(data)
        for k in TIMESTAMPS:
            row[k] = _timestamp(data.get(k))
        for k in FLOATS:
            row[k] = None if data.get(k) is None else float(data[k])
        for k in INTS:
            row[k] = None if data.get(k) is None else int(data[k])
    except (TypeError, ValueError) as exc:
        raise InvalidAlert(f"bad value: {exc}") from None
    return tuple(row.get(c) for c in COLUMNS)


# ------------------------------------------------------------------ storage --

class AlertStore:
    """Writes alert rows to Postgres. Reconnects after a failure."""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = None

    def _connect(self):
        import psycopg2

        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(self.dsn, connect_timeout=5)
        return self.conn

    def ensure_schema(self) -> None:
        conn = self._connect()
        with conn, conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)

    def insert(self, rows: list[tuple]) -> list[tuple]:
        """Insert rows; return (alert_id, severity, alert_type) of the ones that were new."""
        from psycopg2.extras import execute_values

        conn = self._connect()
        try:
            with conn, conn.cursor() as cur:        # `with conn` commits, or rolls back on error
                return execute_values(cur, INSERT_SQL, rows, fetch=True)
        except Exception:
            self.close()                            # drop a broken connection; retry reconnects
            raise

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None


# --------------------------------------------------------------------- loop --

def decode_batch(messages: Iterable) -> list[tuple]:
    """Kafka messages -> unique alert rows. Bad messages are logged and dropped."""
    rows: dict[str, tuple] = {}
    for msg in messages:
        if msg.error():
            log.warning("kafka_message_error", error=str(msg.error()))
            continue
        CONSUMED.inc()
        try:
            row = parse_alert(msg.value())
        except InvalidAlert as exc:
            INVALID.inc()
            log.warning("invalid_alert_skipped", reason=str(exc), partition=msg.partition(),
                        offset=msg.offset())
            continue
        rows[row[0]] = row                          # same alert_id twice in a batch -> once
    return list(rows.values())


def store_with_retry(store: AlertStore, rows: list[tuple], should_stop,
                     sleep=time.sleep, max_backoff_s: float = 30.0) -> bool:
    """Insert until it works. Returns False only if asked to stop while the DB is down."""
    attempt = 0
    while True:
        started = time.perf_counter()
        try:
            new = store.insert(rows)
        except Exception as exc:  # noqa: BLE001 - any DB failure means: wait and retry
            attempt += 1
            DB_ERRORS.inc()
            backoff = min(max_backoff_s, 2 ** min(attempt, 5))
            log.error("store_failed_will_retry", attempt=attempt, rows=len(rows),
                      backoff_s=backoff, error=str(exc).strip().splitlines()[0])
            if should_stop():
                return False
            sleep(backoff)
            continue
        BATCH_SECONDS.observe(time.perf_counter() - started)
        LAST_STORED.set(time.time())
        for _alert_id, severity, alert_type in new:
            STORED.labels(severity=severity, alert_type=alert_type or "UNKNOWN").inc()
        DUPLICATES.inc(len(rows) - len(new))
        if attempt:
            log.info("store_recovered", attempts=attempt + 1)
        for alert_id, severity, _type in new:
            if severity == "CRITICAL":
                log.warning("critical_alert_stored", alert_id=alert_id)
        log.info("batch_stored", rows=len(rows), inserted=len(new), duplicates=len(rows) - len(new))
        return True


def run(consumer, store: AlertStore, should_stop, batch_size: int = BATCH_SIZE) -> None:
    """Main loop: consume -> store -> commit, until should_stop() is true."""
    while not should_stop():
        messages = consumer.consume(num_messages=batch_size, timeout=POLL_TIMEOUT_S)
        if not messages:
            continue
        rows = decode_batch(messages)
        if rows and not store_with_retry(store, rows, should_stop):
            return                                  # stopping with the batch unstored: no commit
        try:
            consumer.commit(asynchronous=False)     # offsets move only after the rows are safe
        except Exception as exc:  # noqa: BLE001 - e.g. a rebalance; the batch is re-read, then skipped as duplicates
            log.warning("commit_failed", error=str(exc))


def build_consumer(bootstrap: str, topic: str):
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": GROUP_ID,
        "client.id": "alert-consumer",
        "enable.auto.commit": False,         # we commit after the insert (see module doc)
        "auto.offset.reset": "earliest",     # first start: take every alert already in Kafka
    })

    def on_assign(_consumer, partitions):
        log.info("partitions_assigned", partitions=[p.partition for p in partitions])

    consumer.subscribe([topic], on_assign=on_assign)
    return consumer


def main() -> int:
    settings = get_settings()
    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    metrics.start_metrics_server(METRICS_PORT)
    store = AlertStore(settings.postgres.dsn)
    while not stopping:                              # Postgres may still be starting
        try:
            store.ensure_schema()
            break
        except Exception as exc:  # noqa: BLE001
            log.error("schema_check_failed_will_retry", error=str(exc).strip().splitlines()[0])
            time.sleep(5)

    topic = settings.kafka.topics.alerts
    consumer = build_consumer(settings.kafka.bootstrap_servers, topic)
    log.info("started", topic=topic, group_id=GROUP_ID, bootstrap=settings.kafka.bootstrap_servers)
    try:
        run(consumer, store, lambda: stopping)
    finally:
        consumer.close()                             # leave the group cleanly (fast rebalance)
        store.close()
        log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
