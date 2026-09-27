"""api/db.py: fail fast while Postgres is down (found in the database-down drill)."""
import pytest

psycopg2 = pytest.importorskip("psycopg2")
from api import db as dbmod  # noqa: E402


def failing_db(monkeypatch, retry_after_s=5.0):
    db = dbmod.Database("host=nowhere dbname=x", retry_after_s=retry_after_s)
    attempts = []

    def get_pool():
        attempts.append(1)
        raise psycopg2.OperationalError('could not translate host name "postgres"')

    monkeypatch.setattr(db, "_get_pool", get_pool)
    return db, attempts


def test_connect_failure_opens_the_breaker(monkeypatch):
    db, attempts = failing_db(monkeypatch)
    for _ in range(3):
        with pytest.raises(dbmod.DatabaseUnavailable, match="translate host name"):
            db.ping()
    assert len(attempts) == 1           # no retry, and later calls fail without connecting


def test_breaker_closes_after_the_wait(monkeypatch):
    db, attempts = failing_db(monkeypatch, retry_after_s=0.0)
    for _ in range(2):
        with pytest.raises(dbmod.DatabaseUnavailable):
            db.ping()
    assert len(attempts) == 2           # tried again once the wait was over
