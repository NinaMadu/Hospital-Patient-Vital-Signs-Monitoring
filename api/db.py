"""PostgreSQL access for the API: a small thread-safe connection pool.

Owner: Member C.
FastAPI runs plain `def` endpoints in a thread pool, so connections are shared through a
psycopg2 ThreadedConnectionPool. The pool is created on first use: the API starts (and
/health can report the problem) even while Postgres is down.
"""
from __future__ import annotations

import threading
import time
from typing import Any

from common.logger import get_logger

log = get_logger("api")


class DatabaseUnavailable(RuntimeError):
    """Postgres cannot be reached; the API answers 503 instead of 500."""


class _BrokenConnection(DatabaseUnavailable):
    """A pooled connection died (e.g. Postgres restarted); a fresh one may work."""


class Database:
    """Connection pool with a short circuit breaker.

    When Postgres is down, even failing takes seconds: with the container stopped, the
    host name `postgres` no longer resolves and each DNS lookup blocks ~4 s (connect_timeout
    does not cover DNS). So after a failed connect the breaker stays open for
    `retry_after_s` and every call fails at once instead of queueing behind the lookup.
    /health then answers 503 quickly and Prometheus still gets ward_postgres_up 0.
    """

    def __init__(self, dsn: str, minconn: int = 1, maxconn: int = 5, retry_after_s: float = 5.0):
        self.dsn = f"{dsn} connect_timeout=3"
        self.minconn, self.maxconn = minconn, maxconn
        self.retry_after_s = retry_after_s
        self._pool = None
        self._lock = threading.Lock()
        self._down_until = 0.0
        self._last_error = ""

    def _get_pool(self):
        from psycopg2.pool import ThreadedConnectionPool

        with self._lock:
            # Checked again here: a caller that waited for the lock while another one's
            # connect failed must not start a second slow attempt.
            if time.monotonic() < self._down_until:
                raise DatabaseUnavailable(f"database unavailable: {self._last_error}")
            if self._pool is None:
                self._pool = ThreadedConnectionPool(self.minconn, self.maxconn, self.dsn)
            return self._pool

    def query(self, sql: str, params: Any = None) -> list[dict]:
        """Run a read-only query; rows come back as dicts.

        If a pooled connection turns out to be dead (Postgres restarted), it is thrown away
        and the query is tried once more on a fresh connection.
        """
        try:
            return self._query(sql, params)
        except _BrokenConnection:
            return self._query(sql, params)

    def _connect(self):
        import psycopg2
        from psycopg2.pool import PoolError

        if time.monotonic() < self._down_until:
            raise DatabaseUnavailable(f"database unavailable: {self._last_error}")
        try:
            pool = self._get_pool()
            return pool, pool.getconn()
        except PoolError as exc:                 # all connections busy: not an outage
            raise DatabaseUnavailable(f"connection pool exhausted: {exc}") from exc
        except psycopg2.Error as exc:
            self._last_error = str(exc).strip().splitlines()[0] if str(exc).strip() else "error"
            self._down_until = time.monotonic() + self.retry_after_s
            self.reset()
            raise DatabaseUnavailable(self._last_error) from exc

    def _query(self, sql: str, params: Any) -> list[dict]:
        import psycopg2
        from psycopg2.extras import RealDictCursor

        pool, conn = self._connect()
        broken = False
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                rows = [dict(r) for r in cur.fetchall()] if cur.description else []
            conn.rollback()                      # read-only: end the transaction
            return rows
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            broken = True
            raise _BrokenConnection(str(exc).strip()) from exc
        finally:
            try:
                pool.putconn(conn, close=broken)
            except Exception:  # noqa: BLE001 - pool already closed by reset()
                pass

    def ping(self) -> None:
        self.query("SELECT 1 AS ok")

    def reset(self) -> None:
        """Drop the pool, so the next call reconnects (after Postgres restarts)."""
        with self._lock:
            if self._pool is not None:
                try:
                    self._pool.closeall()
                except Exception:  # noqa: BLE001
                    pass
                self._pool = None
