# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""DatusOSIAdapter end-to-end (DB-free): load OSI -> validate / list / dry-run."""

import pytest
import yaml
from types import SimpleNamespace

pytest.importorskip("metricflow")

from datus_semantic_core.models import QueryResult
from datus_semantic_osi.adapter import DatusOSIAdapter
from datus_semantic_osi.config import DatusOSIConfig
from datus_semantic_osi.profile import parse_osi_profile, to_core_schema_document


def _core_yaml(legacy_profile: str) -> str:
    return yaml.safe_dump(
        to_core_schema_document(parse_osi_profile(legacy_profile)),
        sort_keys=False,
        allow_unicode=True,
    )


OSI_YAML = """
semantic_model:
  name: commerce_orders
datasets:
  - name: orders
    source:
      table: fact_orders
    primary_key: order_id
    time_dimension:
      name: order_date
      granularity: day
    dimensions:
      - name: order_channel
        expr: order_channel
metrics:
  - name: order_count
    description: "Order count"
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
    subject_path: [commerce, orders]
  - name: average_order_amount
    description: "Average order amount"
    expression: "AVG(order_amount)"
    dataset: orders
    subject_path: [commerce, orders]
  - name: running_order_count
    description: "Running order count"
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
    grain_to_date: month
    custom_extensions:
      - vendor_name: DATUS
        data:
          window_aggregation: sum
    subject_path: [commerce, orders]
  - name: moving_3_month_order_count_avg
    description: "Three-month moving average of order count"
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
    window: 3 months
    custom_extensions:
      - vendor_name: DATUS
        data:
          window_aggregation: avg
    subject_path: [commerce, orders]
  - name: rolling_order_count_level
    description: "Rolling order count level"
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
    window: 3 months
    custom_extensions:
      - vendor_name: DATUS
        data:
          window_aggregation: avg
    subject_path: [commerce, orders]
  - name: moving_window_month_count
    description: "Number of months in the moving window"
    expression: "COUNT(*)"
    dataset: orders
    window: 3 months
    custom_extensions:
      - vendor_name: DATUS
        data:
          window_aggregation: row_count
    subject_path: [commerce, orders]
  - name: running_min_average_order_amount
    description: "Running minimum of monthly average order amount"
    expression: "AVG(order_amount)"
    dataset: orders
    grain_to_date: month
    custom_extensions:
      - vendor_name: DATUS
        data:
          window_aggregation: min
    subject_path: [commerce, orders]
  - name: running_max_average_order_amount
    description: "Running maximum of monthly average order amount"
    expression: "AVG(order_amount)"
    dataset: orders
    grain_to_date: month
    custom_extensions:
      - vendor_name: DATUS
        data:
          window_aggregation: max
    subject_path: [commerce, orders]
  - name: order_count_mom
    description: "Month-over-month order count ratio"
    metric_kind: derived
    expression: "(order_count - order_count_prev) / NULLIF(order_count_prev, 0)"
    inputs:
      - name: order_count
      - name: order_count
        alias: order_count_prev
        offset_window: "1 month"
    subject_path: [commerce, orders]
  - name: previous_month_order_count
    description: "Previous month order count"
    metric_kind: derived
    expression: "previous_month_order_count"
    inputs:
      - name: order_count
        alias: previous_month_order_count
        offset_window: "1 month"
    subject_path: [commerce, orders]
"""


@pytest.fixture
def adapter(tmp_path):
    (tmp_path / "model.yaml").write_text(_core_yaml(OSI_YAML))
    config = DatusOSIConfig(semantic_models_path=str(tmp_path), datasource="starrocks")
    return DatusOSIAdapter(config)


class _FakeExecutor:
    def __init__(self, result=None, sql_rows=None):
        self.result = result or QueryResult()
        self.calls = []
        self.client = SimpleNamespace(sql_client=SimpleNamespace(query=self._query_sql))
        self.sql_rows = sql_rows or []
        self.sql_queries = []

    async def query_metrics(self, metrics, **kwargs):
        self.calls.append({"metrics": list(metrics), **kwargs})
        return self.result

    def _query_sql(self, sql):
        self.sql_queries.append(sql)
        return self.sql_rows


class _FakeBackend:
    has_live_connection = True

    def __init__(self, executor):
        self.executor = executor

    def make_executor(self, model):
        return self.executor


async def test_validate_semantic_passes(adapter):
    result = await adapter.validate_semantic()
    errors = [i for i in result.issues if i.severity == "error"]
    assert result.valid, f"errors: {[e.message for e in errors]}"


async def test_list_metrics_returns_generated_metric(adapter):
    metrics = await adapter.list_metrics()
    names = {m.name for m in metrics}
    assert "order_count" in names


async def test_list_metrics_exposes_osi_structured_metadata(adapter):
    metrics = {m.name: m for m in await adapter.list_metrics()}

    base = metrics["order_count"]
    assert base.path == ["commerce", "orders"]
    assert base.metadata["metric_kind"] == "aggregate"
    assert base.metadata["dataset"] == "orders"
    assert base.metadata["subject_path"] == ["commerce", "orders"]

    mom = metrics["order_count_mom"]
    assert mom.type == "derived"
    assert mom.metadata["metric_kind"] == "derived"
    assert (
        mom.metadata["expr"]
        == "(order_count - order_count_prev) / NULLIF(order_count_prev, 0)"
    )
    assert mom.metadata["offset_window"] == "1 month"
    assert mom.metadata["inputs"] == [
        {"name": "order_count"},
        {
            "name": "order_count",
            "alias": "order_count_prev",
            "offset_window": "1 month",
        },
    ]
    assert mom.metadata["dataset"] == "orders"
    assert "order_channel" in mom.dimensions


async def test_list_metrics_selects_subject_path(adapter):
    metrics = await adapter.list_metrics(path=["commerce"])
    assert {m.name for m in metrics} == {
        "order_count",
        "average_order_amount",
        "running_order_count",
        "moving_3_month_order_count_avg",
        "rolling_order_count_level",
        "moving_window_month_count",
        "running_min_average_order_amount",
        "running_max_average_order_amount",
        "order_count_mom",
        "previous_month_order_count",
    }


def test_offset_derived_metrics_use_current_period_anchor(adapter):
    query_metrics, hidden_anchor_metrics, filter_anchor_metrics = (
        adapter._query_metrics_plan(["order_count_mom"])
    )

    assert query_metrics == ["order_count_mom", "order_count"]
    assert hidden_anchor_metrics == ["order_count"]
    assert filter_anchor_metrics == ["order_count"]


def test_offset_only_metrics_use_referenced_metric_as_anchor(adapter):
    query_metrics, hidden_anchor_metrics, filter_anchor_metrics = (
        adapter._query_metrics_plan(["previous_month_order_count"])
    )

    assert query_metrics == ["previous_month_order_count", "order_count"]
    assert hidden_anchor_metrics == ["order_count"]
    assert filter_anchor_metrics == ["order_count"]


def test_offset_anchor_filter_removes_offset_only_rows(adapter):
    result = QueryResult(
        columns=[
            "metric_time__month",
            "customer_segment",
            "order_count_mom",
            "order_count",
        ],
        data=[
            {
                "metric_time__month": "2025-09-01",
                "customer_segment": "enterprise",
                "order_count_mom": None,
                "order_count": 1.0,
            },
            {
                "metric_time__month": "2025-10-01",
                "customer_segment": "enterprise",
                "order_count_mom": None,
                "order_count": None,
            },
        ],
    )

    filtered = adapter._filter_offset_anchor_rows(
        result,
        hidden_anchor_metrics=["order_count"],
        filter_anchor_metrics=["order_count"],
    )

    assert filtered.columns == [
        "metric_time__month",
        "customer_segment",
        "order_count_mom",
    ]
    assert filtered.data == [
        {
            "metric_time__month": "2025-09-01",
            "customer_segment": "enterprise",
            "order_count_mom": None,
        }
    ]
    assert filtered.metadata["hidden_offset_anchor_metrics"] == ["order_count"]
    assert filtered.metadata["offset_anchor_filtered_rows"] == 1


async def test_get_dimensions_includes_declared_dimension(adapter):
    dims = await adapter.get_dimensions("order_count")
    names = {d.name for d in dims}
    assert "order_channel" in names
    assert "order_date" in names


async def test_query_metrics_dry_run_renders_sql(adapter):
    result = await adapter.query_metrics(["order_count"], dry_run=True)
    sql = result.metadata.get("sql", "") or (
        result.data[0]["sql"] if result.data else ""
    )
    assert "COUNT(DISTINCT order_id)" in sql
    assert "fact_orders" in sql


async def test_query_metrics_dimension_preserving_dry_run_uses_policy_sql(tmp_path):
    (tmp_path / "model.yaml").write_text(
        _core_yaml(
            """
semantic_model:
  name: shop
datasets:
  - name: orders
    source: {table: orders}
    primary_key: order_id
    time_dimension: {name: order_date, granularity: day}
  - name: customers
    source: {table: customers}
    primary_key: customer_id
    dimensions: [{name: customer_name, expr: customer_name}]
relationships:
  - {name: o2c, from: orders, to: customers, from_columns: [customer_id], to_columns: [customer_id]}
metrics:
  - name: order_count
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
"""
        )
    )
    adapter = DatusOSIAdapter(
        DatusOSIConfig(semantic_models_path=str(tmp_path), datasource="duckdb")
    )

    result = await adapter.query_metrics(
        ["order_count"],
        dimensions=["customer_id__customer_name"],
        join_policy="dimension_preserving",
        zero_fill=True,
        dry_run=True,
    )

    sql = result.metadata["sql"]
    assert "LEFT JOIN" in sql
    assert "COALESCE(fact.order_count, 0)" in sql
    assert result.metadata["join_policy"] == "dimension_preserving"


async def test_query_metrics_dimension_preserving_accepts_dimension_expression(
    tmp_path,
):
    (tmp_path / "model.yaml").write_text(
        _core_yaml(
            """
semantic_model:
  name: shop
datasets:
  - name: orders
    source: {table: orders}
    primary_key: order_id
    time_dimension: {name: order_date, granularity: day}
  - name: customers
    source: {table: customers}
    primary_key: customer_id
    dimensions:
      - name: signup_month
        expr: "DATE_TRUNC('month', created_at)"
relationships:
  - {name: o2c, from: orders, to: customers, from_columns: [customer_id], to_columns: [customer_id]}
metrics:
  - name: order_count
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
"""
        )
    )
    adapter = DatusOSIAdapter(
        DatusOSIConfig(semantic_models_path=str(tmp_path), datasource="duckdb")
    )

    result = await adapter.query_metrics(
        ["order_count"],
        dimensions=["customer_id__signup_month"],
        join_policy="dimension_preserving",
        zero_fill=True,
        dry_run=True,
    )

    sql = result.metadata["sql"]
    assert "DATE_TRUNC" in sql
    assert "AS signup_month" in sql
    assert "SELECT dim.signup_month AS signup_month" in sql
    assert "LEFT JOIN" in sql


async def test_query_metrics_match_only_drops_unmatched_joined_dimension(tmp_path):
    (tmp_path / "model.yaml").write_text(
        _core_yaml(
            """
semantic_model:
  name: shop
datasets:
  - name: orders
    source: {table: orders}
    primary_key: order_id
    time_dimension: {name: order_date, granularity: day}
  - name: customers
    source: {table: customers}
    primary_key: customer_id
    dimensions: [{name: customer_name, expr: customer_name}]
relationships:
  - {name: o2c, from: orders, to: customers, from_columns: [customer_id], to_columns: [customer_id]}
metrics:
  - name: order_count
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
"""
        )
    )
    adapter = DatusOSIAdapter(
        DatusOSIConfig(semantic_models_path=str(tmp_path), datasource="duckdb")
    )
    executor = _FakeExecutor(
        QueryResult(
            columns=["customer_name", "order_count"],
            data=[
                {"customer_name": "Alice", "order_count": 2},
                {"customer_name": None, "order_count": 1},
            ],
        )
    )
    adapter._backend = _FakeBackend(executor)

    result = await adapter.query_metrics(
        ["order_count"], dimensions=["customer_id__customer_name"]
    )

    assert result.data == [{"customer_name": "Alice", "order_count": 2}]
    assert result.metadata["join_policy"] == "match_only"
    assert result.metadata["join_policy_filtered_rows"] == 1


async def test_query_metrics_match_only_preserves_zero_filter_metadata(tmp_path):
    (tmp_path / "model.yaml").write_text(
        _core_yaml(
            """
semantic_model:
  name: shop
datasets:
  - name: orders
    source: {table: orders}
    primary_key: order_id
    time_dimension: {name: order_date, granularity: day}
  - name: customers
    source: {table: customers}
    primary_key: customer_id
    dimensions: [{name: customer_name, expr: customer_name}]
relationships:
  - {name: o2c, from: orders, to: customers, from_columns: [customer_id], to_columns: [customer_id]}
metrics:
  - name: order_count
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
"""
        )
    )
    adapter = DatusOSIAdapter(
        DatusOSIConfig(semantic_models_path=str(tmp_path), datasource="duckdb")
    )
    executor = _FakeExecutor(
        QueryResult(
            columns=["customer_name", "order_count"],
            data=[{"customer_name": "Alice", "order_count": 2}],
        )
    )
    adapter._backend = _FakeBackend(executor)

    result = await adapter.query_metrics(
        ["order_count"], dimensions=["customer_id__customer_name"]
    )

    assert result.data == [{"customer_name": "Alice", "order_count": 2}]
    assert result.metadata["join_policy"] == "match_only"
    assert result.metadata["join_policy_filtered_rows"] == 0


async def test_query_metrics_dimension_preserving_uses_dimension_anchor_sql(tmp_path):
    (tmp_path / "model.yaml").write_text(
        _core_yaml(
            """
semantic_model:
  name: shop
datasets:
  - name: orders
    source: {table: orders}
    primary_key: order_id
    time_dimension: {name: order_date, granularity: day}
  - name: customers
    source: {table: customers}
    primary_key: customer_id
    dimensions: [{name: customer_name, expr: customer_name}]
relationships:
  - {name: o2c, from: orders, to: customers, from_columns: [customer_id], to_columns: [customer_id]}
metrics:
  - name: order_count
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
"""
        )
    )
    adapter = DatusOSIAdapter(
        DatusOSIConfig(semantic_models_path=str(tmp_path), datasource="duckdb")
    )
    executor = _FakeExecutor(
        sql_rows=[
            {"customer_name": "Alice", "order_count": 2},
            {"customer_name": "Bob", "order_count": 0},
        ]
    )
    adapter._backend = _FakeBackend(executor)

    result = await adapter.query_metrics(
        ["order_count"],
        dimensions=["customer_id__customer_name"],
        join_policy="dimension_preserving",
        zero_fill=True,
        time_start="2025-01-01",
        time_end="2025-01-31",
        where="order_channel = 'web'",
        order_by=["-customer_id__customer_name"],
    )

    assert result.data == [
        {"customer_name": "Alice", "order_count": 2},
        {"customer_name": "Bob", "order_count": 0},
    ]
    sql = executor.sql_queries[0]
    assert "LEFT JOIN" in sql
    assert "COALESCE(fact.order_count, 0)" in sql
    assert "order_date >= '2025-01-01'" in sql
    assert "order_channel = 'web'" in sql
    assert "ORDER BY customer_name DESC" in sql
    assert result.metadata["join_policy"] == "dimension_preserving"


async def test_query_metrics_dimension_preserving_rejects_unsafe_runtime_sql(tmp_path):
    (tmp_path / "model.yaml").write_text(
        _core_yaml(
            """
semantic_model:
  name: shop
datasets:
  - name: orders
    source: {table: orders}
    primary_key: order_id
    time_dimension: {name: order_date, granularity: day}
  - name: customers
    source: {table: customers}
    primary_key: customer_id
    dimensions: [{name: customer_name, expr: customer_name}]
relationships:
  - {name: o2c, from: orders, to: customers, from_columns: [customer_id], to_columns: [customer_id]}
metrics:
  - name: order_count
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
"""
        )
    )
    adapter = DatusOSIAdapter(
        DatusOSIConfig(semantic_models_path=str(tmp_path), datasource="duckdb")
    )
    adapter._backend = _FakeBackend(_FakeExecutor())

    with pytest.raises(ValueError, match="time_start"):
        await adapter.query_metrics(
            ["order_count"],
            dimensions=["customer_id__customer_name"],
            join_policy="dimension_preserving",
            time_start="2025-01-01'; DROP TABLE orders; --",
        )

    with pytest.raises(ValueError, match="order_by column"):
        await adapter.query_metrics(
            ["order_count"],
            dimensions=["customer_id__customer_name"],
            join_policy="dimension_preserving",
            order_by=["customer_name; DROP TABLE orders"],
        )


async def test_query_metrics_dimension_preserving_rejects_mixed_fact_sources(tmp_path):
    (tmp_path / "model.yaml").write_text(
        _core_yaml(
            """
semantic_model:
  name: shop
datasets:
  - name: orders
    source: {table: orders}
    primary_key: order_id
    time_dimension: {name: order_date, granularity: day}
  - name: refunds
    source: {table: refunds}
    primary_key: refund_id
    time_dimension: {name: refund_date, granularity: day}
  - name: customers
    source: {table: customers}
    primary_key: customer_id
    dimensions: [{name: customer_name, expr: customer_name}]
relationships:
  - {name: o2c, from: orders, to: customers, from_columns: [customer_id], to_columns: [customer_id]}
  - {name: r2c, from: refunds, to: customers, from_columns: [customer_id], to_columns: [customer_id]}
metrics:
  - name: order_count
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
  - name: refund_count
    expression: "COUNT(DISTINCT refund_id)"
    dataset: refunds
"""
        )
    )
    adapter = DatusOSIAdapter(
        DatusOSIConfig(semantic_models_path=str(tmp_path), datasource="duckdb")
    )
    executor = _FakeExecutor(QueryResult(columns=["customer_name"], data=[]))
    adapter._backend = _FakeBackend(executor)

    with pytest.raises(ValueError, match="dimension_preserving"):
        await adapter.query_metrics(
            ["order_count", "refund_count"],
            dimensions=["customer_id__customer_name"],
            join_policy="dimension_preserving",
        )

    assert executor.sql_queries == []
    assert executor.calls == []


async def test_query_metrics_dimension_preserving_falls_back_for_derived_metric(
    tmp_path,
):
    (tmp_path / "model.yaml").write_text(
        _core_yaml(
            """
semantic_model:
  name: shop
datasets:
  - name: orders
    source: {table: orders}
    primary_key: order_id
    time_dimension: {name: order_date, granularity: day}
  - name: customers
    source: {table: customers}
    primary_key: customer_id
    dimensions: [{name: customer_name, expr: customer_name}]
relationships:
  - {name: o2c, from: orders, to: customers, from_columns: [customer_id], to_columns: [customer_id]}
metrics:
  - name: revenue
    expression: "SUM(order_amount)"
    dataset: orders
  - name: order_count
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
  - name: average_order_value
    metric_kind: derived
    expression: "revenue / NULLIF(order_count, 0)"
    inputs: [revenue, order_count]
"""
        )
    )
    adapter = DatusOSIAdapter(
        DatusOSIConfig(semantic_models_path=str(tmp_path), datasource="duckdb")
    )
    executor = _FakeExecutor(QueryResult(columns=["customer_name"], data=[]))
    adapter._backend = _FakeBackend(executor)

    with pytest.raises(ValueError, match="dimension_preserving"):
        await adapter.query_metrics(
            ["average_order_value"],
            dimensions=["customer_id__customer_name"],
            join_policy="dimension_preserving",
        )

    assert executor.sql_queries == []
    assert executor.calls == []


async def test_query_metrics_dimension_preserving_requires_shared_time_dimension(
    tmp_path,
):
    (tmp_path / "model.yaml").write_text(
        _core_yaml(
            """
semantic_model:
  name: shop
datasets:
  - name: orders
    source: {table: orders}
    primary_key: order_id
    time_dimension: {name: order_date, granularity: day}
  - name: customers
    source: {table: customers}
    primary_key: customer_id
    dimensions: [{name: customer_name, expr: customer_name}]
relationships:
  - {name: o2c, from: orders, to: customers, from_columns: [customer_id], to_columns: [customer_id]}
metrics:
  - name: order_count
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
  - name: shipped_order_count
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
    time_dimension: ship_date
"""
        )
    )
    adapter = DatusOSIAdapter(
        DatusOSIConfig(semantic_models_path=str(tmp_path), datasource="duckdb")
    )
    executor = _FakeExecutor(QueryResult(columns=["customer_name"], data=[]))
    adapter._backend = _FakeBackend(executor)

    with pytest.raises(ValueError, match="dimension_preserving"):
        await adapter.query_metrics(
            ["order_count", "shipped_order_count"],
            dimensions=["customer_id__customer_name"],
            join_policy="dimension_preserving",
            time_start="2025-01-01",
        )

    assert executor.sql_queries == []
    assert executor.calls == []


async def test_query_metrics_postprocesses_window_metrics(adapter):
    executor = _FakeExecutor(
        QueryResult(
            columns=["metric_time__month", "order_count", "average_order_amount"],
            data=[
                {
                    "metric_time__month": "2025-01-01",
                    "order_count": 10,
                    "average_order_amount": 50,
                },
                {
                    "metric_time__month": "2025-02-01",
                    "order_count": 20,
                    "average_order_amount": 70,
                },
                {
                    "metric_time__month": "2025-03-01",
                    "order_count": 30,
                    "average_order_amount": 60,
                },
                {
                    "metric_time__month": "2025-04-01",
                    "order_count": 40,
                    "average_order_amount": 90,
                },
            ],
        )
    )
    adapter._backend = _FakeBackend(executor)

    result = await adapter.query_metrics(
        [
            "order_count",
            "running_order_count",
            "moving_3_month_order_count_avg",
            "rolling_order_count_level",
            "moving_window_month_count",
            "average_order_amount",
            "running_min_average_order_amount",
            "running_max_average_order_amount",
        ],
        dimensions=["metric_time__month"],
        time_granularity="month",
    )

    assert executor.calls[0]["metrics"] == ["order_count", "average_order_amount"]
    assert result.columns == [
        "metric_time__month",
        "order_count",
        "average_order_amount",
        "running_order_count",
        "moving_3_month_order_count_avg",
        "rolling_order_count_level",
        "moving_window_month_count",
        "running_min_average_order_amount",
        "running_max_average_order_amount",
    ]
    assert [row["running_order_count"] for row in result.data] == [10, 30, 60, 100]
    assert [row["moving_3_month_order_count_avg"] for row in result.data] == [
        10,
        15,
        20,
        30,
    ]
    assert [row["rolling_order_count_level"] for row in result.data] == [10, 15, 20, 30]
    assert [row["moving_window_month_count"] for row in result.data] == [1, 2, 3, 3]
    assert [row["running_min_average_order_amount"] for row in result.data] == [
        50,
        50,
        50,
        50,
    ]
    assert [row["running_max_average_order_amount"] for row in result.data] == [
        50,
        70,
        70,
        90,
    ]


async def test_query_metrics_window_postprocessing_preserves_backend_row_order(adapter):
    executor = _FakeExecutor(
        QueryResult(
            columns=["metric_time__month", "order_count"],
            data=[
                {"metric_time__month": "2025-03-01", "order_count": 30},
                {"metric_time__month": "2025-01-01", "order_count": 10},
                {"metric_time__month": "2025-02-01", "order_count": 20},
            ],
        )
    )
    adapter._backend = _FakeBackend(executor)

    result = await adapter.query_metrics(
        ["running_order_count"],
        dimensions=["metric_time__month"],
        time_granularity="month",
        order_by=["-metric_time__month"],
    )

    assert [row["metric_time__month"] for row in result.data] == [
        "2025-03-01",
        "2025-01-01",
        "2025-02-01",
    ]
    assert [row["running_order_count"] for row in result.data] == [60, 10, 30]


async def test_query_metrics_row_count_window_does_not_call_executor_with_empty_metrics(
    adapter,
):
    executor = _FakeExecutor(
        QueryResult(
            columns=["metric_time__month", "moving_window_month_count"],
            data=[
                {"metric_time__month": "2025-01-01", "moving_window_month_count": 1},
                {"metric_time__month": "2025-02-01", "moving_window_month_count": 1},
                {"metric_time__month": "2025-03-01", "moving_window_month_count": 1},
            ],
        )
    )
    adapter._backend = _FakeBackend(executor)

    result = await adapter.query_metrics(
        ["moving_window_month_count"],
        dimensions=["metric_time__month"],
        time_granularity="month",
    )

    assert executor.calls[0]["metrics"] == ["moving_window_month_count"]
    assert [row["moving_window_month_count"] for row in result.data] == [1, 2, 3]


async def test_validate_semantic_warns_for_normalized_dataset_alias(tmp_path):
    (tmp_path / "alias.yml").write_text(
        _core_yaml(
            """
datasets:
  - name: orders_alias
    source: {table: fact_orders}
    primary_key: order_id
metrics:
  - name: order_count
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders_alias
"""
        )
    )
    (tmp_path / "canonical.yml").write_text(
        _core_yaml(
            """
datasets:
  - name: fact_orders
    source: {table: fact_orders}
    primary_key: order_id
    time_dimension: {name: order_date, granularity: day}
"""
        )
    )
    adapter = DatusOSIAdapter(
        DatusOSIConfig(semantic_models_path=str(tmp_path), datasource="starrocks")
    )

    result = await adapter.validate_semantic()

    assert result.valid
    warnings = [i.message for i in result.issues if i.severity == "warning"]
    assert any(
        "Collapsed duplicate dataset `orders_alias`" in message for message in warnings
    )
    metrics = await adapter.list_metrics()
    assert metrics[0].metadata["dataset"] == "fact_orders"


async def test_get_dimensions_includes_joined_dimensions(tmp_path):
    (tmp_path / "model.yaml").write_text(
        _core_yaml(
            """
semantic_model:
  name: shop
datasets:
  - name: orders
    source: {table: orders}
    primary_key: order_id
    time_dimension: {name: order_date, granularity: day}
  - name: customers
    source: {table: customers}
    primary_key: customer_id
    dimensions: [{name: region_id, expr: region_id}]
  - name: regions
    source: {table: regions}
    primary_key: region_id
    dimensions: [{name: region_name, expr: region_name}]
relationships:
  - {name: o2c, from: orders, to: customers, from_columns: [customer_id], to_columns: [customer_id]}
  - {name: c2r, from: customers, to: regions, from_columns: [region_id], to_columns: [region_id]}
metrics:
  - name: order_count
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
"""
        )
    )
    adapter = DatusOSIAdapter(
        DatusOSIConfig(semantic_models_path=str(tmp_path), datasource="duckdb")
    )

    dims = await adapter.get_dimensions("order_count")
    names = {d.name for d in dims}
    assert "order_date" in names
    assert "customer_id__region_id" in names
    assert "customer_id__region_id__region_name" in names

    metrics = await adapter.list_metrics()
    assert "customer_id__region_id__region_name" in metrics[0].dimensions
