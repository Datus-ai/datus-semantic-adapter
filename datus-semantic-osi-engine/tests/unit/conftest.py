# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Fake datus-osi-engine binding for unit tests.

Installed into ``sys.modules`` before the adapter's lazy import runs, so unit
tests never need the compiled wheel. The fake mirrors the binding's public
surface exactly: `Engine`, module-level `validate`, `DIALECTS`,
`SPEC_VERSION`, and the error classes with their real attributes. Row shapes
match the `osi_engine::list` machine contract.
"""

from __future__ import annotations

import sys
import types
from typing import Any, Dict, List, Optional

import pytest

METRIC_ROWS = [
    {
        "name": "order_count",
        "kind": "aggregate",
        "datasets": ["orders"],
        "measures": ["order_count"],
        "description": "Number of orders",
    },
    {
        "name": "revenue",
        "kind": "aggregate",
        "datasets": ["orders"],
        "measures": ["revenue"],
        "description": "Total order amount",
    },
]

DIMENSION_ROWS = [
    {"name": "orders.status", "is_time": False, "description": "Order status"},
    {"name": "orders.order_date", "is_time": True, "description": "Order date"},
    {"name": "customers.region", "is_time": False, "description": None},
]

DATASET_ROWS = [
    {
        "name": "orders",
        "source": "main.orders",
        "primary_key": ["order_id"],
        "fields": 5,
        "time_dimensions": ["order_date"],
    },
    {
        "name": "customers",
        "source": "main.customers",
        "primary_key": ["customer_id"],
        "fields": 3,
        "time_dimensions": [],
    },
]

EXECUTE_RESULT = {
    "dialect": "duckdb",
    "sql": "SELECT status, COUNT(*) AS order_count FROM main.orders GROUP BY status",
    "columns": ["status", "order_count"],
    "rows": [{"status": "paid", "order_count": 2}],
    "row_count": 1,
}


class OsiError(Exception):
    def __init__(
        self,
        message: str,
        *,
        code: str = "internal",
        metrics=(),
        candidates=(),
        hint: Optional[str] = None,
        suggested_retry: Optional[Dict[str, Any]] = None,
        detail: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.metrics = list(metrics)
        self.candidates = list(candidates)
        self.hint = hint
        self.suggested_retry = suggested_retry
        self.detail = detail

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "metrics": self.metrics,
            "candidates": self.candidates,
            "hint": self.hint,
            "suggested_retry": self.suggested_retry,
            "detail": self.detail,
        }


class ModelError(OsiError):
    def __init__(self, message, *, issues=(), compile_errors=(), **kwargs):
        super().__init__(message, **kwargs)
        self.issues = list(issues)
        self.compile_errors = list(compile_errors)


class QueryError(OsiError):
    pass


class ExecuteError(OsiError):
    pass


class FakeEngine:
    """Records constructor and call arguments; returns canned rows.

    Program failures per instance via ``fail_with`` (raised by compile and
    execute) after construction, or globally via the module-level
    ``next_engine_fail_with``.
    """

    instances: List["FakeEngine"] = []

    def __init__(
        self,
        model_path: Optional[str] = None,
        model_text: Optional[str] = None,
        connections_path: Optional[str] = None,
        pool_size: int = 8,
    ) -> None:
        self.model_path = model_path
        self.model_text = model_text
        self.connections_path = connections_path
        self.pool_size = pool_size
        self.compile_calls: List[Dict[str, Any]] = []
        self.execute_calls: List[Dict[str, Any]] = []
        self.fail_with: Optional[Exception] = None
        FakeEngine.instances.append(self)

    def datasets(self) -> List[Dict[str, Any]]:
        return [dict(r) for r in DATASET_ROWS]

    def metrics(self) -> List[Dict[str, Any]]:
        return [dict(r) for r in METRIC_ROWS]

    def dimensions(self) -> List[Dict[str, Any]]:
        return [dict(r) for r in DIMENSION_ROWS]

    def compile(self, query, dialect=None, connection=None, pretty=False):
        self.compile_calls.append(
            {"query": query, "dialect": dialect, "connection": connection, "pretty": pretty}
        )
        if self.fail_with is not None:
            raise self.fail_with
        return {"dialect": dialect or "duckdb", "sql": "SELECT 1 AS compiled"}

    def explain(self, query) -> str:
        return "ScanDataset orders"

    def execute(self, query, dialect=None, connection=None, timeout_secs=None, db_path=None):
        self.execute_calls.append(
            {
                "query": query,
                "dialect": dialect,
                "connection": connection,
                "timeout_secs": timeout_secs,
                "db_path": db_path,
            }
        )
        if self.fail_with is not None:
            raise self.fail_with
        return dict(EXECUTE_RESULT)


def _default_validate(model_text: str) -> Dict[str, Any]:
    return {"valid": True, "issues": [], "compile_errors": []}


def _install_fake_binding() -> types.ModuleType:
    module = types.ModuleType("datus_osi_engine")
    module.__osi_fake__ = True
    module.Engine = FakeEngine
    module.validate = _default_validate
    module.SPEC_VERSION = "0.2.0.dev0"
    module.DIALECTS = [
        "duckdb", "starrocks", "clickhouse", "doris", "tidb", "trino",
        "postgres", "mysql", "snowflake", "bigquery", "databricks", "redshift",
    ]
    module.OsiError = OsiError
    module.ModelError = ModelError
    module.QueryError = QueryError
    module.ExecuteError = ExecuteError
    sys.modules["datus_osi_engine"] = module
    return module


FAKE_BINDING = _install_fake_binding()


@pytest.fixture(autouse=True)
def _reset_fake_binding():
    FakeEngine.instances.clear()
    FAKE_BINDING.validate = _default_validate
    yield
    FakeEngine.instances.clear()


@pytest.fixture
def fake_binding():
    return FAKE_BINDING


@pytest.fixture
def model_file(tmp_path):
    path = tmp_path / "model.yaml"
    path.write_text("version: '0.2.0.dev0'\nsemantic_model: []\n")
    return path


@pytest.fixture
def make_adapter(model_file):
    from datus_semantic_osi_engine.adapter import OSIEngineAdapter
    from datus_semantic_osi_engine.config import OSIEngineConfig

    def _make(**overrides):
        kwargs = {"semantic_model_path": str(model_file), **overrides}
        return OSIEngineAdapter(OSIEngineConfig(**kwargs))

    return _make
