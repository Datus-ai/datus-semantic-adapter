# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""Tests for compiling a whole OSI authoring document into a SemanticModelIR."""

from datus_semantic_osi.compiler import compile_document
from datus_semantic_osi.ir import Aggregation, FilterScope, MetricKind
from datus_semantic_osi.profile import parse_osi_profile as parse_osi

OSI_YAML = """
semantic_model:
  name: baisheng_activities
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
      - name: ac_tags
        expr: ac_tags
metrics:
  - name: new_product_activity_count
    description: "5月包含新产品的活动数量"
    expression: "COUNT(DISTINCT ac_code)"
    dataset: new_product_activities
    time_dimension: start_date
"""


def test_compile_document_builds_dataset_with_filter():
    model = compile_document(parse_osi(OSI_YAML))
    assert len(model.datasets) == 1
    ds = model.datasets[0]
    assert ds.name == "new_product_activities"
    assert ds.sql_table == "v_udata_ac_info"
    assert ds.filters[0].scope is FilterScope.DATASET
    assert ds.filters[0].expression == "FIND_IN_SET('1', ac_tags)"
    assert ds.primary_time_dimension == "start_date"


def test_compile_document_builds_metric_with_backing_measure():
    model = compile_document(parse_osi(OSI_YAML))
    assert len(model.metrics) == 1
    metric = model.metrics[0]
    assert metric.name == "new_product_activity_count"
    assert metric.kind is MetricKind.AGGREGATE
    assert metric.dataset == "new_product_activities"
    assert metric.time_dimension == "start_date"
    assert metric.measures[0].agg is Aggregation.COUNT_DISTINCT
    assert metric.measures[0].expr == "ac_code"
