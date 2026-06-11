# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""DatusNativeBackend: IR -> SQL directly (no MetricFlow)."""

from datus_semantic_osi.compiler import compile_document
from datus_semantic_osi.native_backend import DatusNativeBackend
from datus_semantic_osi.profile import parse_osi_profile as parse_osi

AGG = """
semantic_model:
  name: shop
datasets:
  - name: new_product_activities
    source:
      table: v_udata_ac_info
    filters:
      - expression: "FIND_IN_SET('1', ac_tags)"
        scope: dataset
    primary_key: ac_code
    time_dimension:
      name: start_date
      granularity: day
    dimensions:
      - name: ac_channel
        expr: ac_channel
metrics:
  - name: new_product_activity_count
    expression: "COUNT(DISTINCT ac_code)"
    dataset: new_product_activities
  - name: avg_sr
    expression: "AVG(sr_value)"
    dataset: new_product_activities
"""

RATIO = """
semantic_model:
  name: shop
datasets:
  - name: orders
    source:
      table: orders
    primary_key: order_id
metrics:
  - name: avg_order_amount
    expression: "SUM(amount) / COUNT(DISTINCT order_id)"
    dataset: orders
"""


def _backend():
    return DatusNativeBackend()


def test_aggregate_metric_generates_sql():
    model = compile_document(parse_osi(AGG))
    sql = _backend().render_sql(model, metrics=["new_product_activity_count"])
    assert "COUNT(DISTINCT ac_code)" in sql
    assert "FROM v_udata_ac_info" in sql
    assert "FIND_IN_SET('1', ac_tags)" in sql  # dataset filter in WHERE


def test_group_by_dimension():
    model = compile_document(parse_osi(AGG))
    sql = _backend().render_sql(
        model, metrics=["new_product_activity_count"], dimensions=["ac_channel"]
    )
    assert "GROUP BY" in sql.upper()
    assert "ac_channel" in sql


def test_ratio_metric_generates_nullif_sql():
    model = compile_document(parse_osi(RATIO))
    sql = _backend().render_sql(model, metrics=["avg_order_amount"])
    assert "SUM(amount)" in sql
    assert "COUNT(DISTINCT order_id)" in sql
    assert "NULLIF" in sql.upper()


def test_runtime_where_is_appended():
    model = compile_document(parse_osi(AGG))
    sql = _backend().render_sql(
        model,
        metrics=["new_product_activity_count"],
        where="start_date >= '2025-05-01'",
    )
    assert "start_date >= '2025-05-01'" in sql


def test_capabilities_declare_supported_kinds():
    caps = _backend().capabilities
    assert "aggregate" in caps["metric_kinds"]
    assert "ratio" in caps["metric_kinds"]
    # window/cumulative is not supported by native v1
    assert "cumulative" not in caps["metric_kinds"]
