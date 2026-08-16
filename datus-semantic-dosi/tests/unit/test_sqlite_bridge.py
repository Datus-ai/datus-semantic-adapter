# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

import os
import sqlite3

import duckdb
import pytest
import yaml
from datus_semantic_core.exceptions import SemanticCoreException

from datus_semantic_dosi.config import DosiConfig
from datus_semantic_dosi.engine import EngineHandle
from datus_semantic_dosi.sqlite_bridge import duckdb_companion_for_sqlite


@pytest.fixture
def sqlite_db(tmp_path):
    path = tmp_path / "orders.sqlite"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE orders (id INTEGER, amount REAL)")
    conn.execute('CREATE TABLE "odd ""name" (v TEXT)')
    conn.executemany("INSERT INTO orders VALUES (?, ?)", [(1, 10.5), (2, 4.5)])
    conn.execute('INSERT INTO "odd ""name" VALUES (\'x\')')
    conn.commit()
    conn.close()
    return str(path)


def test_companion_exposes_sqlite_tables_as_views(sqlite_db):
    companion = duckdb_companion_for_sqlite(sqlite_db)

    conn = duckdb.connect(companion, read_only=True)
    try:
        rows = conn.execute("SELECT id, amount FROM orders ORDER BY id").fetchall()
        assert rows == [(1, 10.5), (2, 4.5)]
        odd = conn.execute('SELECT v FROM "odd ""name"').fetchall()
        assert odd == [("x",)]
    finally:
        conn.close()


def test_companion_is_cached_until_sqlite_changes(sqlite_db):
    first = duckdb_companion_for_sqlite(sqlite_db)
    first_mtime = os.path.getmtime(first)

    assert duckdb_companion_for_sqlite(sqlite_db) == first
    assert os.path.getmtime(first) == first_mtime

    # Move the SQLite file's mtime past the companion: next call rebuilds.
    os.utime(sqlite_db, (first_mtime + 10, first_mtime + 10))
    rebuilt = duckdb_companion_for_sqlite(sqlite_db)
    assert rebuilt == first
    assert os.path.getmtime(rebuilt) > first_mtime


def test_companion_reflects_new_sqlite_tables_after_rebuild(sqlite_db):
    companion = duckdb_companion_for_sqlite(sqlite_db)

    conn = sqlite3.connect(sqlite_db)
    conn.execute("CREATE TABLE late_arrival (v INTEGER)")
    conn.commit()
    conn.close()
    os.utime(sqlite_db, (os.path.getmtime(companion) + 10,) * 2)

    rebuilt = duckdb_companion_for_sqlite(sqlite_db)
    conn = duckdb.connect(rebuilt, read_only=True)
    try:
        names = {
            row[0]
            for row in conn.execute("SELECT view_name FROM duckdb_views()").fetchall()
        }
    finally:
        conn.close()
    assert "late_arrival" in names


def test_missing_sqlite_file_raises_actionable_error(tmp_path):
    with pytest.raises(SemanticCoreException, match="not found"):
        duckdb_companion_for_sqlite(str(tmp_path / "absent.sqlite"))


def test_empty_sqlite_file_raises_actionable_error(tmp_path):
    path = tmp_path / "empty.sqlite"
    sqlite3.connect(path).close()
    with pytest.raises(SemanticCoreException, match="no tables"):
        duckdb_companion_for_sqlite(str(path))


def test_connections_file_rewrites_sqlite_to_duckdb_companion(sqlite_db, tmp_path):
    model = tmp_path / "model.yaml"
    model.write_text("semantic_model: []\n")
    config = DosiConfig(
        semantic_model_path=str(model),
        db_config={"type": "sqlite", "uri": sqlite_db, "name": "bird"},
        datasource="bird_school",
    )
    handle = EngineHandle(config)

    connections = handle._write_connections_file(config)
    payload = yaml.safe_load(open(connections))
    entry = payload["datasources"]["bird_school"]

    assert entry["type"] == "duckdb"
    assert entry["default"] is True
    assert entry["uri"] == duckdb_companion_for_sqlite(sqlite_db)


def test_connections_file_requires_sqlite_uri(tmp_path):
    model = tmp_path / "model.yaml"
    model.write_text("semantic_model: []\n")
    config = DosiConfig(
        semantic_model_path=str(model),
        db_config={"type": "sqlite"},
        datasource="bird_school",
    )
    handle = EngineHandle(config)

    with pytest.raises(SemanticCoreException, match="uri"):
        handle._write_connections_file(config)


def test_connections_file_passes_other_types_through(tmp_path):
    model = tmp_path / "model.yaml"
    model.write_text("semantic_model: []\n")
    config = DosiConfig(
        semantic_model_path=str(model),
        db_config={"type": "duckdb", "uri": "/tmp/x.duckdb"},
        datasource="duck",
    )
    handle = EngineHandle(config)

    payload = yaml.safe_load(open(handle._write_connections_file(config)))
    assert payload["datasources"]["duck"]["type"] == "duckdb"
    assert payload["datasources"]["duck"]["uri"] == "/tmp/x.duckdb"
