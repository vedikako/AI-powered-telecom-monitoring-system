from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import Json, execute_values
from psycopg2.pool import SimpleConnectionPool

from common.config import settings
from common.logging import get_logger

log = get_logger("common.db")

_pool: SimpleConnectionPool | None = None


def get_pool() -> SimpleConnectionPool:
    global _pool
    if _pool is None:
        last_error: Exception | None = None
        for attempt in range(1, 12):
            try:
                _pool = SimpleConnectionPool(
                    minconn=1,
                    maxconn=8,
                    dsn=settings.database_url,
                )
                conn = _pool.getconn()
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                _pool.putconn(conn)
                log.info("postgres_connected", extra={"event": "db_connect"})
                return _pool
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                sleep_s = min(2**attempt, 20)
                log.warning(
                    "postgres_retry",
                    extra={"event": "db_retry", "error": str(exc), "count": attempt},
                )
                time.sleep(sleep_s)
        raise RuntimeError(f"could not connect to postgres: {last_error}")
    return _pool


@contextmanager
def get_conn() -> Iterator:
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None


__all__ = ["get_conn", "get_pool", "close_pool", "Json", "execute_values"]
