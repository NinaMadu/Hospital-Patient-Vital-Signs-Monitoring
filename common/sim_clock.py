"""Simulated clock: maps real timestamps to simulated days.

Owner: Member A.
Contract: sim_day = floor((ts - SIM_EPOCH) / SIM_DAY_SECONDS), read through common.config.
Used by both simulators, the Spark jobs and the Airflow DAGs so a "day" means the same everywhere.

A day is half-open: day n covers [day_start(n), day_end(n)), so day_end(n) == day_start(n + 1)
and every instant belongs to exactly one day. Instants before SIM_EPOCH give negative days.

    from common.sim_clock import sim_day, day_start, day_end, current_sim_day
    sim_day(datetime(2026, 9, 28, 0, 7, tzinfo=timezone.utc))   # 1  (with 300 s days)
    day_start(1), day_end(1)                                    # 00:05:00, 00:10:00 UTC
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from common.config import get_settings


def _epoch() -> datetime:
    return get_settings().sim_clock.epoch


def _day_length() -> timedelta:
    return timedelta(seconds=get_settings().sim_clock.day_seconds)


def _as_utc(ts: datetime | float | int) -> datetime:
    """Accept an aware datetime, a naive one (taken as UTC) or Unix seconds."""
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def sim_day(ts: datetime | float | int) -> int:
    """The simulated day an instant falls in."""
    # timedelta // timedelta is exact integer floor division (no float rounding at boundaries).
    return (_as_utc(ts) - _epoch()) // _day_length()


def day_start(n: int) -> datetime:
    """First instant of simulated day n (inclusive)."""
    return _epoch() + n * _day_length()


def day_end(n: int) -> datetime:
    """End of simulated day n (exclusive): the first instant of day n + 1."""
    return day_start(n + 1)


def current_sim_day(now: datetime | None = None) -> int:
    """The simulated day right now (or at `now`)."""
    return sim_day(now or datetime.now(timezone.utc))


def sim_day_column(ts_col):
    """Spark Column version of sim_day() for a timestamp column, e.g. event_time.

    Same formula, evaluated by Spark on every row: floor((ts - epoch) / day_seconds).
    """
    from pyspark.sql import functions as F

    s = get_settings().sim_clock
    seconds_since_epoch = ts_col.cast("double") - F.lit(s.epoch.timestamp())
    return F.floor(seconds_since_epoch / s.day_seconds).cast("int")
