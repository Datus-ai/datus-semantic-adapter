# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""Tests for the Datus Semantic IR data model."""

import pytest

from datus_semantic_osi.ir import (
    Aggregation,
    DatasetIR,
    FieldIR,
    FilterIR,
    IdentifierIR,
    MeasureIR,
    MetricIR,
    MetricKind,
    SemanticModelIR,
)


def test_aggregate_metric_carries_backing_measure():
    metric = MetricIR(
        name="activity_count",
        kind=MetricKind.AGGREGATE,
        measures=[
            MeasureIR(
                name="activity_count", agg=Aggregation.COUNT_DISTINCT, expr="ac_code"
            )
        ],
    )
    assert metric.kind is MetricKind.AGGREGATE
    assert metric.measures[0].agg is Aggregation.COUNT_DISTINCT
    assert metric.measures[0].expr == "ac_code"


def test_ratio_metric_records_numerator_and_denominator():
    metric = MetricIR(
        name="paid_rate",
        kind=MetricKind.RATIO,
        numerator="paid_revenue",
        denominator="order_count",
    )
    assert metric.numerator == "paid_revenue"
    assert metric.denominator == "order_count"


def test_dataset_holds_fields_identifiers_and_filters():
    ds = DatasetIR(
        name="paid_orders",
        sql_table="db.orders",
        fields=[FieldIR(name="amount", expr="amount", type="numeric")],
        identifiers=[IdentifierIR(name="order", type="primary", expr="order_id")],
        filters=[FilterIR(expression="status = 'paid'", scope="dataset")],
    )
    assert ds.sql_table == "db.orders"
    assert ds.identifiers[0].type == "primary"
    assert ds.filters[0].scope == "dataset"


def test_semantic_model_aggregates_datasets_and_metrics():
    model = SemanticModelIR(
        datasets=[DatasetIR(name="orders", sql_table="db.orders")],
        metrics=[MetricIR(name="m", kind=MetricKind.AGGREGATE)],
    )
    assert model.datasets[0].name == "orders"
    assert model.metrics[0].name == "m"


def test_unknown_aggregation_is_rejected():
    with pytest.raises(ValueError):
        MeasureIR(name="x", agg="median", expr="amount")
