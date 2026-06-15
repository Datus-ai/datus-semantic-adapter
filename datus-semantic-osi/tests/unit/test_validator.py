# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""Tests for Profile / IR / capability validation."""

import pytest

from datus_semantic_osi.compiler import compile_document
from datus_semantic_osi.errors import OSIValidationError
from datus_semantic_osi.ir import (
    Aggregation,
    DatasetIR,
    FieldIR,
    IdentifierIR,
    MeasureIR,
    MetricIR,
    MetricKind,
    SemanticModelIR,
)
from datus_semantic_osi.profile import parse_osi_profile as parse_osi
from datus_semantic_osi.validator import (
    ensure_valid,
    validate_capabilities,
    validate_ir,
    validate_profile,
)

GOOD = """
semantic_model:
  name: shop
datasets:
  - name: orders
    source:
      table: orders
    primary_key: order_id
    time_dimension:
      name: order_date
      granularity: day
metrics:
  - name: order_count
    expression: "COUNT(DISTINCT order_id)"
    dataset: orders
    time_dimension: order_date
"""


def test_profile_accepts_valid_document():
    assert validate_profile(parse_osi(GOOD)) == []


def test_profile_rejects_dataset_without_source():
    doc = parse_osi(GOOD)
    doc.datasets[0].source.table = None
    issues = validate_profile(doc)
    assert any("source" in i.lower() for i in issues)


def test_profile_rejects_metric_pointing_at_unknown_dataset():
    doc = parse_osi(GOOD)
    doc.metrics[0].dataset = "nonexistent"
    issues = validate_profile(doc)
    assert any("nonexistent" in i for i in issues)


def test_profile_unknown_dataset_with_time_dimension_returns_issue():
    doc = parse_osi(GOOD)
    doc.metrics[0].dataset = "nonexistent"
    doc.metrics[0].time_dimension = "order_date"
    issues = validate_profile(doc)
    assert any("nonexistent" in i for i in issues)


def test_profile_rejects_metric_without_expression_or_ratio_hints():
    doc = parse_osi(GOOD)
    doc.metrics[0].expression = None
    issues = validate_profile(doc)
    assert any("expression" in i.lower() or "numerator" in i.lower() for i in issues)


def test_ir_requires_ratio_numerator_and_denominator():
    model = SemanticModelIR(
        datasets=[DatasetIR(name="d", sql_table="t")],
        metrics=[MetricIR(name="r", kind=MetricKind.RATIO, dataset="d")],
    )
    issues = validate_ir(model)
    assert any("numerator" in i.lower() or "denominator" in i.lower() for i in issues)


def test_ir_flags_duplicate_measure_names():
    m = MeasureIR(name="dup", agg=Aggregation.SUM, expr="a")
    model = SemanticModelIR(
        datasets=[
            DatasetIR(name="d1", sql_table="t1"),
            DatasetIR(name="d2", sql_table="t2"),
        ],
        metrics=[
            MetricIR(name="m1", kind=MetricKind.AGGREGATE, dataset="d1", measures=[m]),
            MetricIR(
                name="m2",
                kind=MetricKind.AGGREGATE,
                dataset="d2",
                measures=[m.model_copy()],
            ),
        ],
    )
    issues = validate_ir(model)
    assert any("dup" in i for i in issues)


def test_identical_duplicate_datasets_are_merged(tmp_path):
    from datus_semantic_osi.profile import load_osi_path

    ds = "{name: orders, source: {table: orders}, primary_key: order_id}"
    (tmp_path / "a.yml").write_text(
        f'datasets: [{ds}]\nmetrics: [{{name: c, expression: "COUNT(*)", dataset: orders}}]\n'
    )
    (tmp_path / "b.yml").write_text(
        f'datasets: [{ds}]\nmetrics: [{{name: s, expression: "SUM(amount)", dataset: orders}}]\n'
    )
    doc = load_osi_path(str(tmp_path), allow_legacy_profile=True)
    # the identical `orders` dataset declared in both files collapses to one
    assert [d.name for d in doc.datasets] == ["orders"]
    assert {m.name for m in doc.metrics} == {"c", "s"}


def test_conflicting_duplicate_datasets_are_kept_for_validation(tmp_path):
    from datus_semantic_osi.profile import load_osi_path

    (tmp_path / "a.yml").write_text(
        "datasets: [{name: orders, source: {table: orders}, primary_key: order_id}]\n"
    )
    (tmp_path / "b.yml").write_text(
        "datasets: [{name: orders, source: {table: other}, primary_key: id}]\n"
    )
    doc = load_osi_path(str(tmp_path), allow_legacy_profile=True)
    assert [d.name for d in doc.datasets] == ["orders", "orders"]  # validator will flag


def test_ir_flags_duplicate_dataset_names():
    # The same physical table backing many datasets is fine, but two datasets
    # must not share a name (each becomes a separate backend data source).
    model = SemanticModelIR(
        datasets=[
            DatasetIR(name="activity_info", sql_table="t"),
            DatasetIR(name="activity_info", sql_table="t"),
        ],
    )
    issues = validate_ir(model)
    assert any("activity_info" in i and "unique" in i.lower() for i in issues)


def test_ir_flags_duplicate_metric_names():
    model = SemanticModelIR(
        datasets=[DatasetIR(name="d", sql_table="t")],
        metrics=[
            MetricIR(
                name="activity_count",
                kind=MetricKind.AGGREGATE,
                dataset="d",
                measures=[MeasureIR(name="m1", agg=Aggregation.COUNT, expr="1")],
            ),
            MetricIR(
                name="activity_count",
                kind=MetricKind.AGGREGATE,
                dataset="d",
                measures=[MeasureIR(name="m2", agg=Aggregation.SUM, expr="x")],
            ),
        ],
    )
    issues = validate_ir(model)
    assert any(
        "activity_count" in i and "metric" in i.lower() and "unique" in i.lower()
        for i in issues
    )


def test_ir_flags_element_type_conflict_across_datasets():
    # `start_date` is a time field in one dataset but categorical in another ->
    # MetricFlow requires one consistent element type across the whole model.
    model = SemanticModelIR(
        datasets=[
            DatasetIR(
                name="d1",
                sql_table="t",
                fields=[
                    FieldIR(
                        name="start_date",
                        expr="start_date",
                        type="time",
                        is_primary_time=True,
                    )
                ],
            ),
            DatasetIR(
                name="d2",
                sql_table="t",
                fields=[
                    FieldIR(name="start_date", expr="start_date", type="categorical")
                ],
            ),
        ],
    )
    issues = validate_ir(model)
    assert any("start_date" in i and "type" in i.lower() for i in issues)


def test_ir_flags_identifier_vs_dimension_conflict_across_datasets():
    # `product_type` is a primary identifier in one dataset but a plain dimension
    # in another -> inconsistent element type.
    model = SemanticModelIR(
        datasets=[
            DatasetIR(
                name="dim",
                sql_table="dim_pt",
                identifiers=[
                    IdentifierIR(
                        name="product_type", type="primary", expr="product_type"
                    )
                ],
            ),
            DatasetIR(
                name="fact",
                sql_table="f",
                fields=[
                    FieldIR(
                        name="product_type", expr="product_type", type="categorical"
                    )
                ],
            ),
        ],
    )
    issues = validate_ir(model)
    assert any(
        "product_type" in i and ("identifier" in i.lower() or "type" in i.lower())
        for i in issues
    )


def test_capabilities_reject_unsupported_metric_kind():
    model = compile_document(parse_osi(GOOD))
    caps = {"metric_kinds": ["ratio"]}  # aggregate not supported
    issues = validate_capabilities(model, caps)
    assert any("aggregate" in i for i in issues)


def test_ensure_valid_raises_business_error():
    with pytest.raises(OSIValidationError):
        ensure_valid(["something is wrong"])
