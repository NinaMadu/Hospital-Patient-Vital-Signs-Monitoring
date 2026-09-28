"""StreamingQueryListener that turns query progress into JSON logs and Pushgateway metrics.

Owner: Member C.

Spark calls onQueryProgress on the driver after every micro-batch of every query (Q1, Q2,
Q3). For each one we log a JSON line and push these gauges (label `query`) to the
Pushgateway, job "spark_streaming":

  ward_streaming_input_rows                 rows read from Kafka in the last batch
  ward_streaming_input_rows_per_second      arrival rate
  ward_streaming_processed_rows_per_second  processing rate (should keep up with arrival)
  ward_streaming_batch_duration_ms          time to run the batch (triggerExecution)
  ward_streaming_batch_id                   last completed batch
  ward_streaming_kafka_lag_records          Kafka records not read yet (latest - end offset)
  ward_streaming_state_rows                 rows kept in state (windows, dedup ids)
  ward_streaming_rows_dropped_by_watermark  late rows dropped in the last batch
  ward_streaming_last_progress_timestamp_seconds   when the query last made progress
  ward_streaming_query_active               1 while running, 0 after it stops

Spark does not commit its Kafka offsets to a consumer group (it keeps them in the
checkpoint), so kafka-exporter cannot see Spark's lag. The lag gauge fills that gap.
"""
from __future__ import annotations

import json
import time

from pyspark.sql.streaming import StreamingQueryListener

from common import metrics
from common.logger import get_logger

JOB = "spark_streaming"

_log = get_logger("spark-streaming")
_REG = metrics.new_registry()
_G = {
    name: metrics.gauge(f"streaming_{name}", doc, ["query"], registry=_REG)
    for name, doc in {
        "input_rows": "Rows read in the last micro-batch",
        "input_rows_per_second": "Input rate of the last micro-batch",
        "processed_rows_per_second": "Processing rate of the last micro-batch",
        "batch_duration_ms": "Duration of the last micro-batch (triggerExecution)",
        "batch_id": "Id of the last completed micro-batch",
        "kafka_lag_records": "Kafka records available but not read yet",
        "state_rows": "Rows held in state stores",
        "rows_dropped_by_watermark": "Late rows dropped by the watermark in the last batch",
        "last_progress_timestamp_seconds": "Unix time of the last progress event",
        "query_active": "1 while the query runs, 0 after it terminated",
    }.items()
}


def kafka_lag(end_offset: str | None, latest_offset: str | None) -> int | None:
    """Sum over partitions of (latest offset in Kafka - offset read up to).

    Both arguments are Spark's JSON strings, e.g. '{"patient-vitals":{"0":120,"1":98}}'.
    """
    if not end_offset or not latest_offset:
        return None
    try:
        end, latest = json.loads(end_offset), json.loads(latest_offset)
    except (TypeError, ValueError):
        return None
    lag = 0
    for topic, partitions in latest.items():
        for partition, offset in partitions.items():
            lag += max(0, int(offset) - int(end.get(topic, {}).get(partition, 0)))
    return lag


def progress_metrics(p) -> dict:
    """The numbers we publish from one StreamingQueryProgress (also used by the tests)."""
    lags = [kafka_lag(s.endOffset, s.latestOffset) for s in p.sources]
    lags = [lag for lag in lags if lag is not None]
    return {
        "query": p.name or str(p.id),
        "batch_id": p.batchId,
        "input_rows": p.numInputRows,
        "input_rows_per_second": _num(p.inputRowsPerSecond),
        "processed_rows_per_second": _num(p.processedRowsPerSecond),
        "batch_duration_ms": (p.durationMs or {}).get("triggerExecution", p.batchDuration),
        "kafka_lag_records": sum(lags) if lags else None,
        "state_rows": sum(op.numRowsTotal for op in p.stateOperators),
        "rows_dropped_by_watermark": sum(op.numRowsDroppedByWatermark for op in p.stateOperators),
        "watermark": (p.eventTime or {}).get("watermark"),
    }


def _num(x) -> float:
    try:
        x = float(x)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if x != x else x          # NaN (no input yet) -> 0


class MetricsListener(StreamingQueryListener):
    """Registered once in app.py: spark.streams.addListener(MetricsListener())."""

    def __init__(self):
        self._names: dict[str, str] = {}   # query id -> name (terminated events lack the name)

    def onQueryStarted(self, event) -> None:
        name = event.name or str(event.id)
        self._names[str(event.id)] = name
        _G["query_active"].labels(query=name).set(1)
        _log.info("query_started", query=name, run_id=str(event.runId))
        metrics.push(JOB, _REG)

    def onQueryProgress(self, event) -> None:
        m = progress_metrics(event.progress)
        q = m["query"]
        for key in ("input_rows", "input_rows_per_second", "processed_rows_per_second",
                    "batch_duration_ms", "batch_id", "state_rows", "rows_dropped_by_watermark"):
            _G[key].labels(query=q).set(m[key] or 0)
        if m["kafka_lag_records"] is not None:
            _G["kafka_lag_records"].labels(query=q).set(m["kafka_lag_records"])
        _G["last_progress_timestamp_seconds"].labels(query=q).set(time.time())
        _G["query_active"].labels(query=q).set(1)
        _log.info("query_progress", **m)
        metrics.push(JOB, _REG)

    def onQueryIdle(self, event) -> None:
        # No new data: still alive, so keep the "last progress" timestamp fresh.
        q = self._names.get(str(event.id), str(event.id))
        _G["last_progress_timestamp_seconds"].labels(query=q).set(time.time())
        metrics.push(JOB, _REG)

    def onQueryTerminated(self, event) -> None:
        q = self._names.get(str(event.id), str(event.id))
        _G["query_active"].labels(query=q).set(0)
        if event.exception:
            _log.error("query_failed", query=q, error=event.exception.splitlines()[0])
        else:
            _log.info("query_stopped", query=q)
        metrics.push(JOB, _REG)
