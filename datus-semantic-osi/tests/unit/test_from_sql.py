# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""Tests for SQL -> OSI metric inference (anchor + join-direction inference)."""

import json

import pytest

from datus_semantic_osi.errors import OSIValidationError
from datus_semantic_osi.from_sql import infer_metric_from_sql

PK = {"orders": ["order_id"], "customers": ["customer_id"], "regions": ["region_id"]}


def _model(doc):
    return doc["semantic_model"][0]


def _datus_hints(obj):
    return json.loads(obj["custom_extensions"][0]["data"])


def test_single_table_aggregate_anchors_on_that_table():
    doc = infer_metric_from_sql(
        "SELECT SUM(amount) AS total FROM orders", primary_keys=PK
    )
    model = _model(doc)
    assert doc["version"] == "0.2.0.dev0"
    assert [d["name"] for d in model["datasets"]] == ["orders"]
    assert _datus_hints(model["metrics"][0])["dataset"] == "orders"
    assert model["metrics"][0]["expression"]["dialects"][0]["expression"] == "SUM(amount)"
    assert not model.get("relationships")


def test_aggregate_on_fact_side_infers_many_to_one_join():
    sql = (
        "SELECT c.region, SUM(o.amount) AS revenue "
        "FROM orders o JOIN customers c ON o.customer_id = c.customer_id "
        "GROUP BY c.region"
    )
    doc = infer_metric_from_sql(sql, primary_keys=PK)
    model = _model(doc)
    # anchor is orders (the aggregated column lives there)
    assert _datus_hints(model["metrics"][0])["dataset"] == "orders"
    assert model["metrics"][0]["expression"]["dialects"][0]["expression"] == "SUM(amount)"
    rel = model["relationships"][0]
    assert rel["from"] == "orders" and rel["to"] == "customers"
    assert rel["from_columns"] == ["customer_id"]
    assert rel["to_columns"] == ["customer_id"]
    # region (from the dim side) is exposed as a dimension on customers
    assert any(
        field["name"] == "region"
        for d in model["datasets"]
        if d["name"] == "customers"
        for field in d.get("fields", [])
    )


def test_aggregate_on_one_side_joining_many_is_rejected_as_fanout():
    # metric on customers (the "one" side) but joining orders (the "many" side)
    sql = (
        "SELECT COUNT(DISTINCT c.customer_id) AS n "
        "FROM customers c JOIN orders o ON o.customer_id = c.customer_id"
    )
    with pytest.raises(OSIValidationError) as exc:
        infer_metric_from_sql(sql, primary_keys=PK)
    assert "fan-out" in str(exc.value).lower() or "many side" in str(exc.value).lower()


def test_count_star_with_join_is_ambiguous():
    sql = "SELECT COUNT(*) AS n FROM orders o JOIN customers c ON o.customer_id = c.customer_id"
    with pytest.raises(OSIValidationError) as exc:
        infer_metric_from_sql(sql, primary_keys=PK)
    assert "count(*)" in str(exc.value).lower() or "distinct" in str(exc.value).lower()


def test_aggregate_spanning_two_tables_is_ambiguous():
    sql = (
        "SELECT SUM(o.amount * c.weight) AS w "
        "FROM orders o JOIN customers c ON o.customer_id = c.customer_id"
    )
    with pytest.raises(OSIValidationError) as exc:
        infer_metric_from_sql(sql, primary_keys=PK)
    assert "grain" in str(exc.value).lower() or "multiple" in str(exc.value).lower()


def test_join_without_known_keys_is_rejected():
    sql = "SELECT SUM(o.amount) FROM orders o JOIN customers c ON o.customer_id = c.customer_id"
    with pytest.raises(OSIValidationError) as exc:
        infer_metric_from_sql(sql, primary_keys={})  # no PK info
    assert "cardinality" in str(exc.value).lower() or "key" in str(exc.value).lower()


def test_detail_query_is_rejected():
    with pytest.raises(OSIValidationError):
        infer_metric_from_sql("SELECT DISTINCT a, b FROM orders", primary_keys=PK)
