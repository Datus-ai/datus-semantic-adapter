# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""Acceptance: OSI -> IR -> MetricFlow YAML must pass real MetricFlow validation.

These run the genuine MetricFlow parser + semantic validator (no DB needed for
parse/semantic) plus an in-memory DuckDB explain for dry-run SQL rendering.
"""

import pytest

from datus_semantic_osi.compiler import compile_document
from datus_semantic_osi.metricflow_backend import lower_to_metricflow
from datus_semantic_osi.profile import parse_osi_profile as parse_osi

pytest.importorskip("metricflow")

# A multi-metric model over the baisheng activity table, exercising
# aggregate / count-distinct / average / ratio metric kinds.
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
      - name: ac_tags
        expr: ac_tags
      - name: sr_value
        expr: sr_value
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
metrics:
  - name: activity_count
    description: "活动数量"
    expression: "COUNT(DISTINCT ac_code)"
    dataset: activities
  - name: new_product_activity_count
    description: "包含新产品的活动数量"
    expression: "COUNT(DISTINCT ac_code)"
    dataset: new_product_activities
  - name: avg_sr_value
    description: "平均 SR"
    expression: "AVG(sr_value)"
    dataset: activities
  - name: total_sr_value
    description: "SR 合计"
    expression: "SUM(sr_value)"
    dataset: activities
"""


def _validate(directory):
    from metricflow.model.parsing.dir_to_model import (
        parse_directory_of_yaml_files_to_model,
    )
    from metricflow.model.model_validator import ModelValidator

    build = parse_directory_of_yaml_files_to_model(str(directory))
    parse_errors = [str(e) for e in build.issues.errors]
    semantic = ModelValidator().validate_model(build.model)
    semantic_errors = [str(e) for e in semantic.issues.errors]
    return build, parse_errors, semantic_errors


def test_generated_yaml_passes_metricflow_validation(tmp_path):
    art = lower_to_metricflow(compile_document(parse_osi(OSI_YAML)))
    art.write(tmp_path)

    build, parse_errors, semantic_errors = _validate(tmp_path)

    assert parse_errors == [], f"parse errors: {parse_errors}"
    assert semantic_errors == [], f"semantic errors: {semantic_errors}"
    # all four metrics made it into the model
    metric_names = {m.name for m in build.model.metrics}
    assert {
        "activity_count",
        "new_product_activity_count",
        "avg_sr_value",
        "total_sr_value",
    } <= metric_names


def test_dry_run_renders_sql_for_each_metric(tmp_path):
    art = lower_to_metricflow(compile_document(parse_osi(OSI_YAML)))
    art.write(tmp_path)
    build, parse_errors, semantic_errors = _validate(tmp_path)
    assert parse_errors == [], f"parse errors: {parse_errors}"
    assert semantic_errors == [], f"semantic errors: {semantic_errors}"

    from metricflow.sql_clients.duckdb import DuckDbSqlClient
    from metricflow.api.metricflow_client import MetricFlowClient

    client = MetricFlowClient(
        sql_client=DuckDbSqlClient(),
        user_configured_model=build.model,
        system_schema="main",
    )
    for metric in [
        "activity_count",
        "new_product_activity_count",
        "avg_sr_value",
        "total_sr_value",
    ]:
        sql = client.explain(
            metrics=[metric]
        ).rendered_sql_without_descriptions.sql_query
        assert "v_udata_ac_info" in sql
        assert "SELECT" in sql.upper()

    # the filtered dataset metric keeps its business filter in the rendered SQL
    filtered_sql = client.explain(
        metrics=["new_product_activity_count"]
    ).rendered_sql_without_descriptions.sql_query
    assert "FIND_IN_SET" in filtered_sql
