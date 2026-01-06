from __future__ import annotations

from typing import Any
import psycopg

from .config import DB_DSN
from .schema import SQL_SCHEMA


def get_conn(*, row_factory: Any = None) -> psycopg.Connection:
    return psycopg.connect(DB_DSN, row_factory=row_factory)


def ensure_schema(conn: psycopg.Connection) -> None:
    conn.execute(SQL_SCHEMA)
    conn.commit()
