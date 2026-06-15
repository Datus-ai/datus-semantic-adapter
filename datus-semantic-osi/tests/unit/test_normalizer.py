# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""Tests for conservative OSI document normalization."""

from datus_semantic_osi.compiler import compile_document
from datus_semantic_osi.metricflow_backend import lower_to_metricflow
from datus_semantic_osi.normalizer import normalize_document
from datus_semantic_osi.profile import load_osi_path, parse_osi_profile as parse_osi
from datus_semantic_osi.validator import validate_ir, validate_profile


def test_normalizer_collapses_unfiltered_duplicate_table_alias():
    doc = parse_osi(
        """
semantic_model: {name: activity_management}
datasets:
  - name: ac_info
    source: {table: v_udata_ac_info}
    primary_key: ac_code
    dimensions:
      - {name: start_date, type: time}
      - {name: ac_name, type: string}
  - name: v_udata_ac_info
    source: {table: v_udata_ac_info}
    primary_key: ac_code
    time_dimension: {name: start_date, granularity: day}
    dimensions:
      - {name: ac_name, type: categorical}
      - {name: sr_value, type: numeric}
  - name: dim_product_type
    source: {table: dim_product_type}
    primary_key: product_type
    dimensions:
      - {name: type_name, type: categorical}
relationships:
  - {name: ac_to_dim, from: ac_info, to: dim_product_type, from_columns: [product_type], to_columns: [product_type]}
  - {name: self_alias, from: ac_info, to: v_udata_ac_info, from_columns: [ac_code], to_columns: [ac_code]}
metrics:
  - name: max_sr
    expression: "MAX(sr_value)"
    dataset: ac_info
    time_dimension: start_date
"""
    )

    result = normalize_document(doc)

    assert result.errors == []
    assert result.dataset_aliases == {"ac_info": "v_udata_ac_info"}
    assert [d.name for d in result.document.datasets] == [
        "v_udata_ac_info",
        "dim_product_type",
    ]
    assert result.document.metrics[0].dataset == "v_udata_ac_info"
    assert result.document.relationships[0].from_dataset == "v_udata_ac_info"
    assert all(r.name != "self_alias" for r in result.document.relationships)

    model = compile_document(result.document)
    assert validate_ir(model) == []
    artifact = lower_to_metricflow(model)
    assert artifact.data_source_docs[0]["data_source"]["name"] == "v_udata_ac_info"
    assert artifact.metric_docs[0]["metric"]["type_params"]["measures"] == [
        "v_udata_ac_info_sr_value_max"
    ]


def test_normalizer_merges_missing_non_conflicting_dimension():
    doc = parse_osi(
        """
datasets:
  - name: alias_orders
    source: {table: orders}
    primary_key: order_id
    dimensions:
      - {name: channel, type: string}
  - name: orders
    source: {table: orders}
    primary_key: order_id
    time_dimension: {name: order_date, granularity: day}
metrics:
  - {name: order_count, expression: "COUNT(DISTINCT order_id)", dataset: alias_orders}
"""
    )

    result = normalize_document(doc)

    assert result.errors == []
    canonical = result.document.datasets[0]
    assert canonical.name == "orders"
    assert [d.name for d in canonical.dimensions] == ["channel"]
    assert result.document.metrics[0].dataset == "orders"


def test_normalizer_keeps_filtered_dataset_as_logical_dataset():
    doc = parse_osi(
        """
datasets:
  - name: paid_orders
    source: {table: orders}
    filters: [{expression: "status = 'paid'", scope: dataset}]
    primary_key: order_id
  - name: orders
    source: {table: orders}
    primary_key: order_id
metrics:
  - {name: paid_order_count, expression: "COUNT(DISTINCT order_id)", dataset: paid_orders}
"""
    )

    result = normalize_document(doc)

    assert result.errors == []
    assert result.dataset_aliases == {}
    assert [d.name for d in result.document.datasets] == ["paid_orders", "orders"]


def test_normalizer_keeps_query_dataset_as_logical_dataset():
    doc = parse_osi(
        """
datasets:
  - name: orders_by_day
    source: {query: "SELECT order_date, COUNT(*) AS rows_count FROM orders GROUP BY order_date"}
  - name: orders
    source: {table: orders}
    primary_key: order_id
metrics:
  - {name: order_count, expression: "COUNT(DISTINCT order_id)", dataset: orders}
"""
    )

    result = normalize_document(doc)

    assert result.errors == []
    assert result.dataset_aliases == {}
    assert [d.name for d in result.document.datasets] == ["orders_by_day", "orders"]


def test_normalizer_reports_conflicting_duplicate_table_alias():
    doc = parse_osi(
        """
datasets:
  - name: order_alias
    source: {table: orders}
    primary_key: id
  - name: orders
    source: {table: orders}
    primary_key: order_id
metrics:
  - {name: order_count, expression: "COUNT(DISTINCT order_id)", dataset: order_alias}
"""
    )

    result = normalize_document(doc)

    assert result.dataset_aliases == {}
    assert any("primary key conflicts" in error for error in result.errors)


def test_load_osi_path_normalize_rewrites_directory(tmp_path):
    (tmp_path / "alias.yml").write_text(
        """
datasets:
  - name: order_alias
    source: {table: orders}
    primary_key: order_id
metrics:
  - {name: order_count, expression: "COUNT(DISTINCT order_id)", dataset: order_alias}
"""
    )
    (tmp_path / "orders.yml").write_text(
        """
datasets:
  - name: orders
    source: {table: orders}
    primary_key: order_id
    time_dimension: {name: order_date, granularity: day}
"""
    )

    doc = load_osi_path(str(tmp_path), normalize=True, allow_legacy_profile=True)

    assert [d.name for d in doc.datasets] == ["orders"]
    assert doc.metrics[0].dataset == "orders"
    assert validate_profile(doc) == []
