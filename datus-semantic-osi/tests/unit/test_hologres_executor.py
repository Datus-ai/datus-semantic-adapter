# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""A `type: hologres` datasource must execute through MetricFlow's PostgreSQL client.

Hologres speaks the PostgreSQL wire protocol and has no MetricFlow dialect of its
own. `datus_semantic_osi.dialects` already maps it to postgres for expression
parsing (see test_dialects.py); this covers the execution half — the live
executor built by MetricFlowBackend must land on the PostgreSQL client instead of
failing with `Got dialect 'hologres'`.

No warehouse is contacted: SQLAlchemy engines are constructed lazily, so building
the client needs no reachable host or valid credentials.
"""

import pytest

from datus_semantic_osi.backend import MetricFlowBackend
from datus_semantic_osi.ir import DatasetIR, SemanticModelIR

pytest.importorskip("metricflow")
pytest.importorskip("psycopg2")

HOLOGRES_DB_CONFIG = {
    "type": "hologres",
    "host": "holo-host",
    "port": "80",
    "username": "holo_user",
    "password": "holo_pw",
    "database": "njyh",
}


def _model() -> SemanticModelIR:
    return SemanticModelIR(
        name="orders_model",
        datasets=[DatasetIR(name="orders", sql_table="public.orders")],
    )


def _executor(tmp_path, **db_config_overrides):
    backend = MetricFlowBackend(
        generated_path=str(tmp_path),
        db_config={**HOLOGRES_DB_CONFIG, **db_config_overrides},
        datasource="njyh",
    )
    return backend.make_executor(_model())


def test_live_executor_selects_postgres_client(tmp_path):
    from metricflow.sql_clients.postgres import PostgresSqlClient

    executor = _executor(tmp_path)

    assert isinstance(executor.client.sql_client, PostgresSqlClient)


def test_live_executor_defaults_schema_to_public(tmp_path):
    executor = _executor(tmp_path)

    assert executor.client.system_schema == "public"


def test_live_executor_honors_explicit_schema(tmp_path):
    executor = _executor(tmp_path, schema="njyh_mart")

    assert executor.client.system_schema == "njyh_mart"


def test_live_executor_preserves_connection_details(tmp_path):
    executor = _executor(tmp_path, sslmode="require")

    # No public engine accessor on the MetricFlow client; the URL is the only
    # place the resolved connection details are observable without connecting.
    url = executor.client.sql_client._engine.url
    assert url.drivername == "postgresql+psycopg2"
    assert url.host == "holo-host"
    assert url.port == 80
    assert url.database == "njyh"
    assert url.username == "holo_user"
    # The password travels separately from the URL — make_sql_client_from_config
    # passes it as the second argument to from_connection_details — so a
    # regression that drops it would otherwise still satisfy this test.
    assert url.password == "holo_pw"
    assert url.query.get("sslmode") == "require"
