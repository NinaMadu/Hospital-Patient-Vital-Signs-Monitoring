"""Shared objects handed to the endpoints through FastAPI's Depends().

Owner: Member C.
Tests swap them out with app.dependency_overrides[get_repo] = lambda: FakeRepo().
"""
from __future__ import annotations

from functools import lru_cache

from api.db import Database
from api.repository import WardRepository
from common.config import get_settings


@lru_cache(maxsize=1)
def get_repo() -> WardRepository:
    return WardRepository(Database(get_settings().postgres.dsn))


def stale_after_seconds() -> float:
    """A patient with no reading for this long is shown as stale (config: health)."""
    return float(get_settings().health.no_vitals_alert_seconds)
