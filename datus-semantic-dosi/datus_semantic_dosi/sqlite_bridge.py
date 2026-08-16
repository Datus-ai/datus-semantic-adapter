# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""SQLite → DuckDB companion bridge for engine execution.

The Dosi engine deliberately has no SQLite dialect ("sqlite is a Datus
built-in but not an osi dialect; use duckdb"), but Datus projects legitimately
run on SQLite datasources. This module builds the bridge the engine's own
hint prescribes: a cached DuckDB companion database whose views expose every
SQLite table through ``sqlite_scan``, so the engine executes against a plain
DuckDB connection while the data stays in the original SQLite file.

The companion contains views only — no copied data — so it is cheap to
rebuild and is regenerated whenever the SQLite file's mtime moves past it.
Reading the views requires DuckDB's ``sqlite`` extension, which current
DuckDB builds autoinstall/autoload on first use.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import tempfile
from pathlib import Path

from datus_semantic_core.exceptions import SemanticCoreException

_BRIDGE_DIR_NAME = "dosi-sqlite-bridge"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sqlite_tables(sqlite_path: str) -> list[str]:
    with sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        ).fetchall()
    return [row[0] for row in rows]


def _companion_path(sqlite_path: str) -> Path:
    digest = hashlib.sha256(sqlite_path.encode("utf-8")).hexdigest()[:16]
    stem = Path(sqlite_path).stem or "db"
    bridge_dir = Path(tempfile.gettempdir()) / _BRIDGE_DIR_NAME
    bridge_dir.mkdir(parents=True, exist_ok=True)
    return bridge_dir / f"{stem}-{digest}.duckdb"


def duckdb_companion_for_sqlite(sqlite_source: str) -> str:
    """Return a DuckDB database path exposing the SQLite file's tables as views.

    The companion is cached per absolute SQLite path and rebuilt when it is
    missing or older than the SQLite file. Raises ``SemanticCoreException``
    with an actionable message when the SQLite file cannot be read or the
    ``duckdb`` package is unavailable.
    """
    sqlite_path = os.path.abspath(os.path.expanduser(sqlite_source))
    if not os.path.isfile(sqlite_path):
        raise SemanticCoreException(
            f"SQLite datasource file not found: {sqlite_path!r}"
        )

    companion = _companion_path(sqlite_path)
    if companion.exists() and companion.stat().st_mtime >= os.path.getmtime(
        sqlite_path
    ):
        return str(companion)

    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - guarded by dependency
        raise SemanticCoreException(
            "The `duckdb` package is required to execute Dosi queries against "
            "a SQLite datasource (it bridges SQLite into the engine's DuckDB "
            "connector); install it with `pip install duckdb`."
        ) from exc

    tables = _sqlite_tables(sqlite_path)
    if not tables:
        raise SemanticCoreException(
            f"SQLite datasource {sqlite_path!r} contains no tables to expose"
        )

    # Build into a sibling temp file, then atomically replace, so a concurrent
    # reader never observes a half-written companion.
    scratch = companion.with_name(companion.name + f".tmp{os.getpid()}")
    scratch.unlink(missing_ok=True)
    conn = duckdb.connect(str(scratch))
    try:
        for table in tables:
            conn.execute(
                f"CREATE VIEW {_quote_ident(table)} AS "
                f"SELECT * FROM sqlite_scan({_quote_literal(sqlite_path)}, {_quote_literal(table)})"
            )
    finally:
        conn.close()
    os.replace(scratch, companion)
    return str(companion)
