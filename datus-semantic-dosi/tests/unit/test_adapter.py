# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Adapter behavior against the fake binding: query construction, slicing,
dry-run shape, engine lifecycle, and connections wiring."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml
from _fakes import FakeEngine, QueryError
from datus_semantic_core.exceptions import SemanticCoreException
from datus_semantic_dosi.errors import SemanticValidationException


def _write_model(path: Path, model_name: str, metric_names: list[str]) -> None:
    metrics = "\n".join(f"      - name: {name}" for name in metric_names)
    path.write_text(
        "version: '0.2.0.dev0'\n"
        "semantic_model:\n"
        f"  - name: {model_name}\n"
        "    datasets: []\n"
        "    metrics:\n"
        f"{metrics}\n"
    )


def _install_file_catalog(monkeypatch, metrics_by_stem: dict[str, list[str]]) -> None:
    def metrics(engine):
        stem = Path(engine.model_path).stem
        return [
            {
                "name": name,
                "kind": "aggregate",
                "datasets": [stem],
                "measures": [name],
                "time_dimension": f"{stem}.event_date",
                "description": f"Metric from {stem}",
            }
            for name in metrics_by_stem[stem]
        ]

    def datasets(engine):
        stem = Path(engine.model_path).stem
        return [
            {
                "name": stem,
                "source": f"main.{stem}",
                "primary_key": ["id"],
                "fields": 2,
                "time_dimensions": ["event_date"],
                "primary_time_dimension": "event_date",
            }
        ]

    def dimensions(engine):
        stem = Path(engine.model_path).stem
        return [
            {"name": f"{stem}.status", "is_time": False},
            {
                "name": f"{stem}.event_date",
                "is_time": True,
                "time_granularity": "day",
            },
        ]

    monkeypatch.setattr(FakeEngine, "metrics", metrics)
    monkeypatch.setattr(FakeEngine, "datasets", datasets)
    monkeypatch.setattr(FakeEngine, "dimensions", dimensions)


async def test_list_metrics_maps_rows_and_slices(make_adapter):
    adapter = make_adapter()
    metrics = await adapter.list_metrics()
    assert [m.name for m in metrics] == [
        "order_count",
        "revenue",
        "running_revenue",
    ]
    assert metrics[0].type == "aggregate"
    assert metrics[0].measures == ["order_count"]
    assert metrics[0].metadata == {
        "datasets": ["orders"],
        "base_kind": "aggregate",
        "datus_ext_version": "1.2",
        "time_dimension": "orders.order_date",
    }
    assert metrics[0].dimensions == []

    assert [m.name for m in await adapter.list_metrics(limit=1)] == ["order_count"]
    assert [m.name for m in await adapter.list_metrics(limit=5, offset=1)] == [
        "revenue",
        "running_revenue",
    ]


async def test_list_metrics_exposes_native_window_metadata(make_adapter, model_file):
    model_file.write_text(
        """
version: 0.2.0.dev0
semantic_model:
  - name: orders_model
    datasets: []
    metrics:
      - name: running_revenue
        expression:
          dialects: [{dialect: ANSI_SQL, expression: 'SUM(orders.amount)'}]
        custom_extensions:
          - vendor_name: DATUS
            data: >-
              {"v":"1.2",
              "window":{"type":"cumulative","function":"sum"},
              "subject_path":["sales","revenue"],"unit":"USD"}
""".lstrip()
    )
    metrics = {metric.name: metric for metric in await make_adapter().list_metrics()}
    running = metrics["running_revenue"]
    assert running.type == "window"
    assert running.path == ["sales", "revenue"]
    assert running.unit == "USD"
    assert running.metadata["requires_time_axis"] is True
    assert running.metadata["window"] == {
        "type": "cumulative",
        "function": "sum",
    }


async def test_window_axis_probe_uses_configured_target(make_adapter):
    adapter = make_adapter(dialect="starrocks", connection="warehouse")

    await adapter.get_dimensions("running_revenue")

    calls = FakeEngine.instances[-1].compile_calls
    assert calls
    assert {call["dialect"] for call in calls} == {"starrocks"}
    assert {call["connection"] for call in calls} == {"warehouse"}


async def test_list_metrics_exposes_native_window_family_and_axis(
    make_adapter, model_file, monkeypatch
):
    model_file.write_text(
        """
version: 0.2.0.dev0
semantic_model:
  - name: orders_model
    datasets: []
    metrics:
      - name: order_rank
        custom_extensions:
          - vendor_name: DATUS
            data: '{"v":"1.3","window":{"rank":{"function":"rank"}}}'
      - name: first_revenue
        custom_extensions:
          - vendor_name: DATUS
            data: '{"v":"1.3","window":{"value":{"function":"first_value"}}}'
      - name: revenue_range
        custom_extensions:
          - vendor_name: DATUS
            data: >-
              {"v":"1.3","window":{"frame":{"function":"count",
              "preceding":"unbounded","units":"range",
              "order":{"by":"value","direction":"asc"}}}}
""".lstrip()
    )
    rows = [
        {
            "name": "order_rank",
            "kind": "aggregate",
            "datasets": ["orders"],
            "measures": ["revenue"],
            "window_family": "rank",
            "window_function": "rank",
        },
        {
            "name": "first_revenue",
            "kind": "aggregate",
            "datasets": ["orders"],
            "measures": ["revenue"],
            "window_family": "value",
            "window_function": "first_value",
        },
        {
            "name": "revenue_range",
            "kind": "aggregate",
            "datasets": ["orders"],
            "measures": ["revenue"],
            "window_family": "frame",
            "window_function": "count",
        },
    ]
    monkeypatch.setattr(FakeEngine, "metrics", lambda self: [dict(row) for row in rows])
    monkeypatch.setattr(FakeEngine, "time_axis_metrics", {"first_revenue"})

    metrics = {metric.name: metric for metric in await make_adapter().list_metrics()}
    assert metrics["order_rank"].metadata["window_family"] == "rank"
    assert metrics["order_rank"].metadata["window_function"] == "rank"
    assert metrics["order_rank"].metadata["requires_time_axis"] is False
    assert metrics["first_revenue"].metadata["requires_time_axis"] is True
    assert metrics["revenue_range"].metadata["requires_time_axis"] is False


async def test_get_dimensions_returns_queryable_and_flags_time(make_adapter):
    adapter = make_adapter()
    dims = {d.name: d for d in await adapter.get_dimensions("revenue")}
    assert set(dims) == {"orders.status", "orders.order_date", "customers.region"}
    assert dims["orders.order_date"].type == "time"
    assert dims["orders.order_date"].is_primary_time is True
    assert dims["orders.order_date"].time_granularities == [
        "day",
        "week",
        "month",
        "quarter",
        "year",
    ]
    assert dims["orders.status"].type is None
    assert dims["orders.status"].is_primary_time is False


async def test_get_dimensions_keeps_grains_for_other_time_dimensions(
    make_adapter, monkeypatch
):
    original_dimensions = FakeEngine.dimensions

    def dimensions(self):
        return original_dimensions(self) + [
            {
                "name": "customers.signup_date",
                "is_time": True,
                "time_granularity": "month",
                "description": "Signup month",
            }
        ]

    monkeypatch.setattr(FakeEngine, "dimensions", dimensions)

    dims = {d.name: d for d in await make_adapter().get_dimensions("revenue")}

    assert dims["customers.signup_date"].time_granularities == [
        "month",
        "quarter",
        "year",
    ]


async def test_unknown_native_grain_is_left_to_planner_probing(
    make_adapter, monkeypatch
):
    original_dimensions = FakeEngine.dimensions

    def dimensions(self):
        rows = original_dimensions(self)
        for row in rows:
            if row["name"] == "orders.order_date":
                row["time_granularity"] = "hour"
        return rows

    monkeypatch.setattr(FakeEngine, "dimensions", dimensions)

    dims = {d.name: d for d in await make_adapter().get_dimensions("revenue")}

    assert dims["orders.order_date"].time_granularities == [
        "day",
        "week",
        "month",
        "quarter",
        "year",
    ]


async def test_missing_axis_grains_returns_structured_error(make_adapter, monkeypatch):
    import datus_semantic_dosi.adapter as adapter_module

    monkeypatch.setattr(adapter_module, "queryable_grains", lambda _grain: [])

    with pytest.raises(SemanticValidationException) as exc:
        await make_adapter().get_dimensions("running_revenue")

    assert exc.value.payload.code == "no_primary_time_dimension"
    assert exc.value.payload.metrics == ["running_revenue"]


async def test_get_dimensions_returns_only_native_queryable_dimensions(
    make_adapter, monkeypatch
):
    original_compile = FakeEngine.compile

    def compile_query(self, query, dialect=None, connection=None, pretty=False):
        if query["group_by"] == [{"field": "customers.region"}]:
            raise QueryError(
                "multiple relationship paths",
                code="ambiguous_join_path",
            )
        return original_compile(
            self,
            query,
            dialect=dialect,
            connection=connection,
            pretty=pretty,
        )

    monkeypatch.setattr(FakeEngine, "compile", compile_query)

    dimensions = await make_adapter().get_dimensions("revenue")

    assert [dimension.name for dimension in dimensions] == [
        "orders.status",
        "orders.order_date",
    ]


async def test_window_dimension_discovery_includes_required_time_axis(make_adapter):
    dimensions = {
        dimension.name: dimension
        for dimension in await make_adapter().get_dimensions("running_revenue")
    }

    assert "orders.status" in dimensions
    assert "customers.region" in dimensions
    assert dimensions["orders.order_date"].time_granularities == [
        "day",
        "week",
        "month",
        "quarter",
        "year",
    ]


async def test_window_dimension_discovery_probes_each_grain(make_adapter, monkeypatch):
    original_compile = FakeEngine.compile

    def compile_query(self, query, dialect=None, connection=None, pretty=False):
        metric_time = next(
            (
                item
                for item in query.get("group_by") or []
                if item.get("field") == "metric_time"
            ),
            None,
        )
        if metric_time and metric_time.get("grain") in {"quarter", "year"}:
            raise QueryError(
                "reset is finer than the query grain",
                code="window_reset_too_fine",
            )
        return original_compile(
            self,
            query,
            dialect=dialect,
            connection=connection,
            pretty=pretty,
        )

    monkeypatch.setattr(FakeEngine, "compile", compile_query)

    dimensions = {
        dimension.name: dimension
        for dimension in await make_adapter().get_dimensions("running_revenue")
    }

    assert dimensions["orders.order_date"].time_granularities == [
        "day",
        "week",
        "month",
    ]


async def test_get_dimensions_unknown_metric_is_structured(make_adapter):
    adapter = make_adapter()
    with pytest.raises(SemanticValidationException) as exc:
        await adapter.get_dimensions("revenues")
    payload = exc.value.payload
    assert payload.code == "unknown_metric"
    assert payload.metrics == ["revenues"]
    assert "order_count" in payload.message


async def test_query_metrics_builds_metric_query(make_adapter):
    adapter = make_adapter()
    await adapter.query_metrics(
        metrics=["revenue"],
        dimensions=["orders.status", "orders.order_date"],
        time_start="2025-01-01",
        time_end="2025-02-01",
        time_granularity="month",
        where="status <> 'void'",
        limit=10,
        order_by=["-revenue", "status"],
    )
    engine = FakeEngine.instances[-1]
    (call,) = engine.execute_calls
    assert call["query"] == {
        "metrics": ["revenue"],
        "group_by": [
            {"field": "orders.status"},
            {"field": "orders.order_date", "grain": "month"},
        ],
        "where_sql": "status <> 'void'",
        "time_range": {"start": "2025-01-01", "end": "2025-02-01"},
        "order_by": [
            {"key": "revenue", "desc": True},
            {"key": "status", "desc": False},
        ],
        "limit": 10,
    }
    assert call["timeout_secs"] == 30.0
    assert call["connection"] is None


async def test_query_metrics_bare_time_dimension_gets_grain(make_adapter):
    adapter = make_adapter()
    await adapter.query_metrics(
        metrics=["revenue"], dimensions=["order_date"], time_granularity="day"
    )
    engine = FakeEngine.instances[-1]
    assert engine.execute_calls[0]["query"]["group_by"] == [
        {"field": "order_date", "grain": "day"}
    ]


async def test_query_metrics_uses_native_metric_time_input(make_adapter):
    adapter = make_adapter()
    await adapter.query_metrics(
        metrics=["running_revenue"],
        dimensions=["metric_time"],
        time_granularity="month",
    )
    engine = FakeEngine.instances[-1]
    assert engine.execute_calls[0]["query"]["group_by"] == [
        {"field": "metric_time", "grain": "month"}
    ]


async def test_bare_ambiguous_dimension_is_forwarded_to_dosi(make_adapter):
    adapter = make_adapter()

    await adapter.query_metrics(metrics=["revenue"], dimensions=["status"])

    engine = FakeEngine.instances[-1]
    assert engine.execute_calls[0]["query"]["group_by"] == [{"field": "status"}]


async def test_time_range_without_time_grouping_binds_metric_time_dimension(
    make_adapter,
):
    """A time filter with no time grouping resolves the metric's time dimension.

    The engine otherwise rejects the query with time_range_needs_dimension —
    but "total for September" style asks are the most common Datus shape.
    """
    adapter = make_adapter()
    await adapter.query_metrics(
        metrics=["revenue"], time_start="2025-09-01", time_end="2025-10-01"
    )
    engine = FakeEngine.instances[-1]
    assert engine.execute_calls[0]["query"]["time_range"] == {
        "start": "2025-09-01",
        "end": "2025-10-01",
        "dimension": "metric_time",
    }


def test_time_range_with_ambiguous_time_dimensions_delegates_to_metric_time(
    make_adapter,
):
    adapter = make_adapter()
    query = adapter._build_query(
        [
            {"name": "orders.order_date", "is_time": True},
            {"name": "orders.ship_date", "is_time": True},
        ],
        [{"name": "revenue", "datasets": ["orders"]}],
        metrics=["revenue"],
        dimensions=[],
        time_start="2025-09-01",
        time_end="2025-10-01",
        time_granularity=None,
        where=None,
        limit=None,
        order_by=None,
    )
    assert query["time_range"]["dimension"] == "metric_time"


def test_time_range_with_no_reachable_time_dimension_uses_metric_time(make_adapter):
    """The engine reports no_primary_time_dimension from its compiled IR."""
    adapter = make_adapter()
    query = adapter._build_query(
        [{"name": "orders.status", "is_time": False}],
        [{"name": "revenue", "datasets": ["orders"]}],
        metrics=["revenue"],
        dimensions=[],
        time_start="2025-09-01",
        time_end="2025-10-01",
        time_granularity=None,
        where=None,
        limit=None,
        order_by=None,
    )
    assert query["time_range"] == {
        "start": "2025-09-01",
        "end": "2025-10-01",
        "dimension": "metric_time",
    }


async def test_time_granularity_without_time_dimension_is_structured(make_adapter):
    adapter = make_adapter()
    with pytest.raises(SemanticValidationException) as exc:
        await adapter.query_metrics(
            metrics=["revenue"], dimensions=["orders.status"], time_granularity="day"
        )
    payload = exc.value.payload
    assert payload.code == "time_grain_required"
    assert payload.required_dimensions[0] == "metric_time"
    assert payload.suggested_retry == {
        "metrics": ["revenue"],
        "dimensions": ["orders.status", "metric_time"],
        "time_granularity": "day",
    }


async def test_dry_run_returns_sql_contract(make_adapter):
    adapter = make_adapter(db_config={"type": "postgresql"})
    result = await adapter.query_metrics(metrics=["revenue"], dry_run=True)
    assert result.columns == ["sql"]
    assert result.data == [{"sql": "SELECT 1 AS compiled"}]
    assert result.metadata["dry_run"] is True
    assert result.metadata["sql"] == "SELECT 1 AS compiled"
    engine = FakeEngine.instances[-1]
    (call,) = engine.compile_calls
    # postgresql (Datus vocabulary) normalized to postgres (engine dialect)
    assert call["dialect"] == "postgres"
    assert call["pretty"] is True
    assert not engine.execute_calls


async def test_execute_result_maps_to_query_result(make_adapter):
    adapter = make_adapter()
    result = await adapter.query_metrics(
        metrics=["order_count"], dimensions=["orders.status"]
    )
    assert result.columns == ["status", "order_count"]
    assert result.data == [{"status": "paid", "order_count": 2}]
    assert result.metadata["row_count"] == 1
    assert "sql" in result.metadata


async def test_engine_rebuilds_on_model_mtime_change(make_adapter, model_file):
    adapter = make_adapter()
    await adapter.list_metrics()
    await adapter.list_metrics()
    assert len(FakeEngine.instances) == 1

    model_file.write_text("version: '0.2.0.dev0'\nsemantic_model: []\n# touched\n")
    os.utime(model_file, (0, 0))  # force a different mtime regardless of clock
    await adapter.list_metrics()
    assert len(FakeEngine.instances) == 2


async def test_db_config_written_as_datasources_yaml(make_adapter):
    adapter = make_adapter(
        db_config={"type": "postgresql", "host": "db.local", "port": 5432},
        datasource="warehouse",
    )
    await adapter.query_metrics(metrics=["revenue"])
    engine = FakeEngine.instances[-1]
    assert engine.execute_calls[0]["connection"] == "warehouse"
    with open(engine.connections_path, encoding="utf-8") as fh:
        payload = yaml.safe_load(fh)
    assert payload == {
        "datasources": {
            "warehouse": {
                "type": "postgres",
                "host": "db.local",
                "port": 5432,
                "default": True,
            }
        }
    }


async def test_explicit_connections_path_wins(make_adapter, tmp_path):
    connections = tmp_path / "agent.yml"
    connections.write_text("datasources: {}\n")
    adapter = make_adapter(
        connections_path=str(connections),
        db_config={"type": "mysql"},
        connection="prod",
    )
    await adapter.query_metrics(metrics=["revenue"])
    engine = FakeEngine.instances[-1]
    assert engine.connections_path == str(connections)
    assert engine.execute_calls[0]["connection"] == "prod"


def test_list_semantic_models_maps_datasets(make_adapter):
    adapter = make_adapter()
    models = adapter.list_semantic_models()
    assert [m.name for m in models] == ["orders", "customers"]
    assert models[0].table_name == "main.orders"
    assert models[0].extra["primary_key"] == ["order_id"]

    assert adapter.get_semantic_model("orders").name == "orders"
    assert adapter.get_semantic_model("main.customers").name == "customers"
    assert adapter.get_semantic_model("nope") is None


async def test_validate_semantic_maps_issues(make_adapter, fake_binding):
    def failing_validate(text):
        return {
            "valid": False,
            "issues": [
                {
                    "severity": "warning",
                    "code": "missing_sql_dialect",
                    "location": "semantic_model[0]",
                    "message": "field has no SQL dialect",
                }
            ],
            "compile_errors": [
                {
                    "code": "unknown_column",
                    "location": "metrics[0]",
                    "message": "no such column",
                    "hint": "did you mean amount?",
                }
            ],
        }

    fake_binding.validate = failing_validate
    adapter = make_adapter()
    result = await adapter.validate_semantic()
    assert result.valid is False
    assert len(result.issues) == 2
    assert result.issues[0].severity == "warning"
    assert result.issues[0].location == "semantic_model[0]"
    assert "unknown_column" in result.issues[1].message
    assert "did you mean amount?" in result.issues[1].message


async def test_validate_semantic_ok(make_adapter):
    adapter = make_adapter()
    result = await adapter.validate_semantic()
    assert result.valid is True
    assert result.issues == []


async def test_validate_semantic_returns_structured_invalid_yaml_issue(
    make_adapter, model_file
):
    model_file.write_text("semantic_model: [\n")

    result = await make_adapter().validate_semantic()

    assert result.valid is False
    assert len(result.issues) == 1
    assert "invalid_yaml" in result.issues[0].message


async def test_validate_semantic_rejects_legacy_window_hints(make_adapter, model_file):
    model_file.write_text(
        """
version: 0.2.0.dev0
semantic_model:
  - name: orders_model
    datasets: []
    metrics:
      - name: legacy_revenue
        expression:
          dialects: [{dialect: ANSI_SQL, expression: 'SUM(orders.amount)'}]
        custom_extensions:
          - vendor_name: DATUS
            data: '{"grain_to_date":"year","window_aggregation":"sum"}'
""".lstrip()
    )

    result = await make_adapter().validate_semantic()

    assert result.valid is False
    assert len(result.issues) == 1
    assert "legacy_window_hint" in result.issues[0].message


async def test_validate_semantic_supports_targeted_single_model(
    make_adapter, model_file
):
    model_file.write_text(
        "version: '0.2.0.dev0'\nsemantic_model:\n  - name: activity_management\n"
    )
    adapter = make_adapter()

    matched = await adapter.validate_semantic(
        scope="semantic_model",
        semantic_model_name="activity_management",
    )
    missing = await adapter.validate_semantic(
        scope="semantic_model",
        semantic_model_name="missing_model",
    )

    assert matched.valid is True
    assert matched.issues == []
    assert missing.valid is False
    assert len(missing.issues) == 1
    assert "semantic_model_not_found" in missing.issues[0].message


async def test_semantic_models_path_directory_single_file(tmp_path):
    from datus_semantic_dosi.adapter import DosiAdapter
    from datus_semantic_dosi.config import DosiConfig

    (tmp_path / "model.yaml").write_text("version: '0.2.0.dev0'\nsemantic_model: []\n")
    adapter = DosiAdapter(DosiConfig(semantic_models_path=str(tmp_path)))
    await adapter.list_metrics()  # builds the engine
    assert FakeEngine.instances[-1].model_path == str(tmp_path / "model.yaml")


async def test_semantic_models_path_routes_metrics_across_files(tmp_path, monkeypatch):
    from datus_semantic_dosi.adapter import DosiAdapter
    from datus_semantic_dosi.config import DosiConfig

    _write_model(tmp_path / "orders.yaml", "orders_model", ["order_count"])
    _write_model(tmp_path / "users.yml", "users_model", ["active_users"])
    _install_file_catalog(
        monkeypatch,
        {"orders": ["order_count"], "users": ["active_users"]},
    )
    adapter = DosiAdapter(DosiConfig(semantic_models_path=str(tmp_path)))

    assert [metric.name for metric in await adapter.list_metrics()] == [
        "order_count",
        "active_users",
    ]
    assert [
        metric.name for metric in await adapter.list_metrics(limit=1, offset=1)
    ] == ["active_users"]
    assert {model.name for model in adapter.list_semantic_models()} == {
        "orders",
        "users",
    }

    await adapter.get_dimensions("active_users")
    users_engine = next(
        engine
        for engine in FakeEngine.instances
        if Path(engine.model_path).name == "users.yml"
    )
    assert users_engine.compile_calls

    await adapter.query_metrics(metrics=["order_count"], dry_run=True)
    orders_engine = next(
        engine
        for engine in FakeEngine.instances
        if Path(engine.model_path).name == "orders.yaml"
    )
    assert orders_engine.compile_calls


async def test_semantic_models_path_rejects_cross_model_query(tmp_path, monkeypatch):
    from datus_semantic_dosi.adapter import DosiAdapter
    from datus_semantic_dosi.config import DosiConfig

    _write_model(tmp_path / "orders.yaml", "orders_model", ["order_count"])
    _write_model(tmp_path / "users.yaml", "users_model", ["active_users"])
    _install_file_catalog(
        monkeypatch,
        {"orders": ["order_count"], "users": ["active_users"]},
    )
    adapter = DosiAdapter(DosiConfig(semantic_models_path=str(tmp_path)))

    with pytest.raises(SemanticValidationException) as exc:
        await adapter.query_metrics(metrics=["order_count", "active_users"])

    assert exc.value.payload.code == "cross_semantic_model_query_unsupported"
    assert exc.value.payload.metrics == ["order_count", "active_users"]
    assert "order_count -> orders_model" in exc.value.payload.message
    assert "active_users -> users_model" in exc.value.payload.message


async def test_semantic_models_path_rejects_duplicate_metric_names(
    tmp_path, monkeypatch
):
    from datus_semantic_dosi.adapter import DosiAdapter
    from datus_semantic_dosi.config import DosiConfig

    _write_model(tmp_path / "orders.yaml", "orders_model", ["total"])
    _write_model(tmp_path / "users.yaml", "users_model", ["total"])
    _install_file_catalog(monkeypatch, {"orders": ["total"], "users": ["total"]})
    adapter = DosiAdapter(DosiConfig(semantic_models_path=str(tmp_path)))

    with pytest.raises(SemanticCoreException, match="metric 'total'.*must be unique"):
        await adapter.list_metrics()


async def test_semantic_models_path_refreshes_when_file_is_added(tmp_path, monkeypatch):
    from datus_semantic_dosi.adapter import DosiAdapter
    from datus_semantic_dosi.config import DosiConfig

    metrics_by_stem = {"orders": ["order_count"]}
    _write_model(tmp_path / "orders.yaml", "orders_model", ["order_count"])
    _install_file_catalog(monkeypatch, metrics_by_stem)
    adapter = DosiAdapter(DosiConfig(semantic_models_path=str(tmp_path)))
    assert [metric.name for metric in await adapter.list_metrics()] == ["order_count"]

    metrics_by_stem["users"] = ["active_users"]
    _write_model(tmp_path / "users.yaml", "users_model", ["active_users"])

    assert [metric.name for metric in await adapter.list_metrics()] == [
        "order_count",
        "active_users",
    ]


async def test_semantic_model_path_remains_an_explicit_single_file_pin(
    tmp_path, monkeypatch
):
    from datus_semantic_dosi.adapter import DosiAdapter
    from datus_semantic_dosi.config import DosiConfig

    orders = tmp_path / "orders.yaml"
    _write_model(orders, "orders_model", ["order_count"])
    _write_model(tmp_path / "users.yaml", "users_model", ["active_users"])
    _install_file_catalog(
        monkeypatch,
        {"orders": ["order_count"], "users": ["active_users"]},
    )
    adapter = DosiAdapter(
        DosiConfig(
            semantic_model_path=str(orders),
            semantic_models_path=str(tmp_path),
        )
    )

    assert [metric.name for metric in await adapter.list_metrics()] == ["order_count"]
    with pytest.raises(SemanticValidationException) as exc:
        await adapter.query_metrics(metrics=["active_users"])
    assert exc.value.payload.code == "unknown_metric"


async def test_validate_semantic_validates_all_files_and_supports_targeting(
    tmp_path, fake_binding
):
    from datus_semantic_dosi.adapter import DosiAdapter
    from datus_semantic_dosi.config import DosiConfig

    _write_model(tmp_path / "orders.yaml", "orders_model", ["order_count"])
    _write_model(tmp_path / "users.yaml", "users_model", ["active_users"])
    validated = []

    def validate(model_text):
        validated.append(model_text)
        if "users_model" in model_text:
            return {
                "valid": False,
                "issues": [
                    {
                        "severity": "error",
                        "code": "invalid_users_model",
                        "location": "semantic_model[0]",
                        "message": "users model is invalid",
                    }
                ],
                "compile_errors": [],
            }
        return {"valid": True, "issues": [], "compile_errors": []}

    fake_binding.validate = validate
    adapter = DosiAdapter(DosiConfig(semantic_models_path=str(tmp_path)))

    all_models = await adapter.validate_semantic()
    assert all_models.valid is False
    assert len(validated) == 2
    assert "[semantic_model_file=users.yaml]" in all_models.issues[0].message

    validated.clear()
    orders_only = await adapter.validate_semantic(
        scope="semantic_model",
        semantic_model_name="orders_model",
    )
    assert orders_only.valid is True
    assert len(validated) == 1
    assert "orders_model" in validated[0]


async def test_validate_semantic_rejects_cross_file_duplicate_identities(tmp_path):
    from datus_semantic_dosi.adapter import DosiAdapter
    from datus_semantic_dosi.config import DosiConfig

    _write_model(tmp_path / "a.yaml", "shared_model", ["shared_metric"])
    _write_model(tmp_path / "b.yaml", "shared_model", ["shared_metric"])
    adapter = DosiAdapter(DosiConfig(semantic_models_path=str(tmp_path)))

    result = await adapter.validate_semantic()

    assert result.valid is False
    messages = [issue.message for issue in result.issues]
    assert any("duplicate_semantic_model" in message for message in messages)
    assert any("duplicate_metric" in message for message in messages)


async def test_targeted_validation_rejects_duplicate_metric_in_sibling_file(
    tmp_path,
):
    from datus_semantic_dosi.adapter import DosiAdapter
    from datus_semantic_dosi.config import DosiConfig

    _write_model(tmp_path / "orders.yaml", "orders_model", ["shared_metric"])
    _write_model(tmp_path / "users.yaml", "users_model", ["shared_metric"])
    _write_model(tmp_path / "audit.yaml", "audit_model", ["audit_count"])
    adapter = DosiAdapter(DosiConfig(semantic_models_path=str(tmp_path)))

    result = await adapter.validate_semantic(
        scope="semantic_model",
        semantic_model_name="orders_model",
    )

    assert result.valid is False
    assert any("duplicate_metric" in issue.message for issue in result.issues)

    unrelated = await adapter.validate_semantic(
        scope="semantic_model",
        semantic_model_name="audit_model",
    )
    assert unrelated.valid is True
    assert unrelated.issues == []
