# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""DatusOSIAdapter end-to-end (DB-free): load OSI -> validate / list / dry-run."""

import pytest
import yaml

pytest.importorskip("metricflow")

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
  name: baisheng_activities
datasets:
  - name: activities
    source:
      table: v_udata_ac_info
    primary_key: ac_code
    time_dimension:
      name: start_date
      granularity: day
    dimensions:
      - name: ac_channel
        expr: ac_channel
metrics:
  - name: activity_count
    description: "活动数量"
    expression: "COUNT(DISTINCT ac_code)"
    dataset: activities
    subject_path: [baisheng, activity]
  - name: activity_count_mom
    description: "活动数环比"
    metric_kind: derived
    expression: "(activity_count - activity_count_prev) / NULLIF(activity_count_prev, 0)"
    inputs:
      - name: activity_count
      - name: activity_count
        alias: activity_count_prev
        offset_window: "1 month"
    subject_path: [baisheng, activity]
"""


@pytest.fixture
def adapter(tmp_path):
    (tmp_path / "model.yaml").write_text(_core_yaml(OSI_YAML))
    config = DatusOSIConfig(semantic_models_path=str(tmp_path), datasource="starrocks")
    return DatusOSIAdapter(config)


async def test_validate_semantic_passes(adapter):
    result = await adapter.validate_semantic()
    errors = [i for i in result.issues if i.severity == "error"]
    assert result.valid, f"errors: {[e.message for e in errors]}"


async def test_list_metrics_returns_generated_metric(adapter):
    metrics = await adapter.list_metrics()
    names = {m.name for m in metrics}
    assert "activity_count" in names


async def test_list_metrics_exposes_osi_structured_metadata(adapter):
    metrics = {m.name: m for m in await adapter.list_metrics()}

    base = metrics["activity_count"]
    assert base.path == ["baisheng", "activity"]
    assert base.metadata["metric_kind"] == "aggregate"
    assert base.metadata["dataset"] == "activities"
    assert base.metadata["subject_path"] == ["baisheng", "activity"]

    mom = metrics["activity_count_mom"]
    assert mom.type == "derived"
    assert mom.metadata["metric_kind"] == "derived"
    assert (
        mom.metadata["expr"]
        == "(activity_count - activity_count_prev) / NULLIF(activity_count_prev, 0)"
    )
    assert mom.metadata["offset_window"] == "1 month"
    assert mom.metadata["inputs"] == [
        {"name": "activity_count"},
        {
            "name": "activity_count",
            "alias": "activity_count_prev",
            "offset_window": "1 month",
        },
    ]
    assert mom.metadata["dataset"] == "activities"
    assert "ac_channel" in mom.dimensions


async def test_list_metrics_filters_subject_path(adapter):
    metrics = await adapter.list_metrics(path=["baisheng"])
    assert {m.name for m in metrics} == {"activity_count", "activity_count_mom"}


async def test_get_dimensions_includes_declared_dimension(adapter):
    dims = await adapter.get_dimensions("activity_count")
    names = {d.name for d in dims}
    assert "ac_channel" in names
    assert "start_date" in names


async def test_query_metrics_dry_run_renders_sql(adapter):
    result = await adapter.query_metrics(["activity_count"], dry_run=True)
    sql = result.metadata.get("sql", "") or (
        result.data[0]["sql"] if result.data else ""
    )
    assert "COUNT(DISTINCT ac_code)" in sql
    assert "v_udata_ac_info" in sql


async def test_validate_semantic_warns_for_normalized_dataset_alias(tmp_path):
    (tmp_path / "alias.yml").write_text(
        _core_yaml(
            """
datasets:
  - name: ac_info
    source: {table: v_udata_ac_info}
    primary_key: ac_code
metrics:
  - name: activity_count
    expression: "COUNT(DISTINCT ac_code)"
    dataset: ac_info
"""
        )
    )
    (tmp_path / "canonical.yml").write_text(
        _core_yaml(
            """
datasets:
  - name: v_udata_ac_info
    source: {table: v_udata_ac_info}
    primary_key: ac_code
    time_dimension: {name: start_date, granularity: day}
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
        "Collapsed duplicate dataset `ac_info`" in message for message in warnings
    )
    metrics = await adapter.list_metrics()
    assert metrics[0].metadata["dataset"] == "v_udata_ac_info"


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
