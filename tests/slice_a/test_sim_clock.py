"""Simulated clock: day numbers, boundaries and settings (common/sim_clock.py)."""
from datetime import datetime, timedelta, timezone

import pytest

from common import sim_clock
from common.config import get_settings

EPOCH = datetime(2026, 9, 28, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def clock(monkeypatch):
    """Pin the clock to a 300 s day from 2026-09-28 00:00 UTC, whatever .env says."""
    monkeypatch.setenv("SIM_EPOCH", EPOCH.isoformat())
    monkeypatch.setenv("SIM_DAY_SECONDS", "300")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_epoch_is_day_zero():
    assert sim_clock.sim_day(EPOCH) == 0


@pytest.mark.parametrize("offset_s, day", [
    (0, 0), (299, 0), (299.999999, 0), (300, 1), (301, 1), (3000, 10), (-1, -1), (-300, -1), (-301, -2),
])
def test_day_boundaries(offset_s, day):
    assert sim_clock.sim_day(EPOCH + timedelta(seconds=offset_s)) == day


def test_start_and_end_are_half_open():
    assert sim_clock.day_start(1) == EPOCH + timedelta(minutes=5)
    assert sim_clock.day_end(1) == sim_clock.day_start(2)
    assert sim_clock.sim_day(sim_clock.day_start(7)) == 7
    assert sim_clock.sim_day(sim_clock.day_end(7) - timedelta(microseconds=1)) == 7
    assert sim_clock.sim_day(sim_clock.day_end(7)) == 8


def test_accepts_naive_datetime_unix_seconds_and_other_timezones():
    later = EPOCH + timedelta(seconds=650)
    ist = timezone(timedelta(hours=5, minutes=30))
    assert sim_clock.sim_day(later.replace(tzinfo=None)) == 2
    assert sim_clock.sim_day(later.timestamp()) == 2
    assert sim_clock.sim_day(later.astimezone(ist)) == 2


def test_current_sim_day():
    assert sim_clock.current_sim_day(EPOCH + timedelta(hours=1)) == 12
    assert isinstance(sim_clock.current_sim_day(), int)


def test_reads_day_length_and_epoch_from_settings(monkeypatch):
    monkeypatch.setenv("SIM_DAY_SECONDS", "60")
    monkeypatch.setenv("SIM_EPOCH", "2026-10-01T00:00:00+00:00")
    get_settings.cache_clear()
    assert sim_clock.sim_day(datetime(2026, 10, 1, 0, 2, 30, tzinfo=timezone.utc)) == 2
    assert sim_clock.day_start(0) == datetime(2026, 10, 1, tzinfo=timezone.utc)
