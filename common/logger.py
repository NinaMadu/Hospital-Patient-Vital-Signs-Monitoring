"""Structured JSON logger used by every component.

Owner: Member C.
Contract: one JSON object per line with at least timestamp, component, event, severity,
plus identifiers such as patient_id or sim_day when relevant.

Usage:
    from common.logger import get_logger
    log = get_logger("alert-consumer")
    log.info("alert_stored", patient_id="P007", rule="SPO2_LOW", sim_day=3)
    log.warning("db_retry", attempt=2)
    try: ...
    except Exception:
        log.exception("batch_failed", batch_id=12)      # adds error + traceback fields

    -> {"timestamp": "2026-09-28T08:00:00.123+00:00", "component": "alert-consumer",
        "event": "alert_stored", "severity": "INFO", "patient_id": "P007", "rule": "SPO2_LOW",
        "sim_day": 3}

Built on the standard `logging` module, so levels work as usual (LOG_LEVEL=DEBUG) and
Airflow, which captures stdout, shows the lines in its task logs unchanged.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any

# WARNING is printed as WARN to match the producer's existing log lines.
_SEVERITY = {"WARNING": "WARN", "CRITICAL": "CRITICAL"}
_RESERVED = ("timestamp", "component", "event", "severity")
_LOGGING_KWARGS = ("exc_info", "stack_info", "stacklevel", "extra")


class JsonFormatter(logging.Formatter):
    """LogRecord -> one JSON line. Extra fields come from record.fields."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc)
                                 .isoformat(timespec="milliseconds"),
            "component": getattr(record, "component", record.name),
            "event": record.getMessage(),
            "severity": _SEVERITY.get(record.levelname, record.levelname),
        }
        for key, value in getattr(record, "fields", {}).items():
            # A field may not overwrite the four contract keys.
            payload[f"field_{key}" if key in _RESERVED else key] = value
        if record.exc_info and record.exc_info[0] is not None:
            exc_type, exc, tb = record.exc_info
            payload["error"] = f"{exc_type.__name__}: {exc}"
            payload["traceback"] = "".join(traceback.format_exception(exc_type, exc, tb))
        return json.dumps(payload, default=str)


class _StdoutHandler(logging.StreamHandler):
    """Writes to whatever sys.stdout is *now* (Airflow swaps it while a task runs)."""

    def __init__(self):
        super().__init__(sys.stdout)

    def emit(self, record: logging.LogRecord) -> None:
        self.stream = sys.stdout
        super().emit(record)


class EventLogger(logging.LoggerAdapter):
    """log.info("event_name", key=value, ...): keyword arguments become JSON fields."""

    def process(self, msg: Any, kwargs: dict[str, Any]):
        fields = {k: kwargs.pop(k) for k in list(kwargs) if k not in _LOGGING_KWARGS}
        extra = dict(kwargs.pop("extra", None) or {})
        extra["component"] = self.extra["component"]
        extra["fields"] = {**self.extra["context"], **fields}
        kwargs["extra"] = extra
        return msg, kwargs

    def bind(self, **context: Any) -> "EventLogger":
        """A logger that adds these fields to every line, e.g. log.bind(sim_day=3)."""
        return EventLogger(self.logger, {"component": self.extra["component"],
                                         "context": {**self.extra["context"], **context}})


def get_logger(component: str, **context: Any) -> EventLogger:
    """JSON logger for one component. `context` fields are added to every line."""
    logger = logging.getLogger(f"ward.{component}")
    if not getattr(logger, "_ward_configured", False):
        handler = _StdoutHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
        logger.propagate = False      # no second, non-JSON copy through the root logger
        logger._ward_configured = True
    return EventLogger(logger, {"component": component, "context": context})
