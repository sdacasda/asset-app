"""Database helper boundary for future refactors."""

import sqlite3
from typing import Iterable

from .config import settings


def connect(db_path: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or settings.db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows: Iterable[sqlite3.Row] = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [row["name"] for row in rows]
