# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""Best-effort migration: legacy MetricFlow YAML -> OSI authoring + report."""

from datus_semantic_osi.compiler import compile_document
from datus_semantic_osi.ir import MetricKind
from datus_semantic_osi.migrate import migrate_metricflow_yaml
from datus_semantic_osi.profile import parse_osi

MF_YAML = """
data_source:
  name: orders
  sql_table: shop.orders
  identifiers:
    - name: order
      type: primary
      expr: order_id
  dimensions:
    - name: ds
      type: time
      type_params:
        is_primary: true
        time_granularity: day
      expr: created_at
    - name: status
      type: categorical
  measures:
    - name: order_count
      agg: count_distinct
      expr: order_id
    - name: amount_sum
      agg: sum
      expr: amount
---
metric:
  name: total_orders
  type: measure_proxy
  type_params:
    measures:
      - order_count
---
metric:
  name: avg_amount
  type: expr
  type_params:
    expr: amount_sum / NULLIF(order_count, 0)
    measures:
      - amount_sum
      - order_count
---
metric:
  name: orders_last_month
  type: cumulative
  type_params:
    measures:
      - order_count
    window: 1 month
---
metric:
  name: filtered_orders
  type: measure_proxy
  type_params:
    measures:
      - order_count
  constraint: |
    is_vip
"""


def test_migration_recovers_dataset_and_time_dimension():
    osi, _report = migrate_metricflow_yaml(MF_YAML)
    doc = parse_osi(osi)
    ds = doc.datasets[0]
    assert ds.name == "orders"
    assert ds.source.table == "shop.orders"
    assert ds.time_dimension.name == "ds"
    assert ds.primary_key == "order_id"


def test_measure_proxy_becomes_business_expression():
    osi, _ = migrate_metricflow_yaml(MF_YAML)
    model = compile_document(parse_osi(osi))
    metric = {m.name: m for m in model.metrics}["total_orders"]
    assert metric.kind is MetricKind.AGGREGATE
    assert metric.measures[0].expr == "order_id"
    assert metric.measures[0].agg.value == "count_distinct"


def test_cumulative_window_is_preserved():
    osi, _ = migrate_metricflow_yaml(MF_YAML)
    doc = parse_osi(osi)
    metric = {m.name: m for m in doc.metrics}["orders_last_month"]
    assert metric.window == "1 month"


def test_constraint_is_reported_as_needing_hints():
    _osi, report = migrate_metricflow_yaml(MF_YAML)
    assert any(
        "filtered_orders" in item and "constraint" in item.lower() for item in report
    )
