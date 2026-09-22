# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Engine exception -> core error mapping matrix."""

from __future__ import annotations

import pytest
from _fakes import (
    ExecuteError,
    FakeEngine,
    ModelError,
    NotComputableError,
    QueryError,
)
from datus_semantic_core.exceptions import SemanticCoreException
from datus_semantic_dosi.errors import (
    SemanticValidationException,
    raise_mapped,
    validation_error_from_query_error,
)


async def test_query_error_becomes_validation_exception(make_adapter):
    adapter = make_adapter()
    await adapter.list_metrics()  # builds the engine
    error = QueryError(
        'unknown dimension "regionn"',
        code="unknown_dimension",
        candidates=["customers.region"],
    )
    FakeEngine.instances[-1].fail_with = error
    with pytest.raises(SemanticValidationException) as exc:
        await adapter.query_metrics(metrics=["revenue"], dimensions=["regionn"])
    payload = exc.value.payload
    assert payload.code == "unknown_dimension"
    assert payload.metrics == ["revenue"]
    assert payload.unsupported_dimensions == ["regionn"]
    assert payload.suggested_retry == {
        "metrics": ["revenue"],
        "dimensions": ["customers.region"],
    }
    assert "customers.region" in payload.message


@pytest.mark.parametrize(
    "code",
    ["unknown_relationship", "invalid_relationship_path"],
)
async def test_relationship_path_error_becomes_validation_exception(make_adapter, code):
    adapter = make_adapter()
    await adapter.list_metrics()
    error = QueryError(
        "invalid relationship-prefixed dimension",
        code=code,
        candidates=["orders_to_customers"],
        hint="use a relationship that leaves the current dataset",
    )
    FakeEngine.instances[-1].fail_with = error

    with pytest.raises(SemanticValidationException) as exc:
        await adapter.query_metrics(
            metrics=["revenue"], dimensions=["bad_relationship.region"]
        )

    payload = exc.value.payload
    assert payload.code == code
    assert payload.metrics == ["revenue"]
    assert payload.unsupported_dimensions == []
    assert payload.suggested_retry is None
    assert "orders_to_customers" in payload.message
    assert "use a relationship that leaves the current dataset" in payload.message


async def test_execute_error_becomes_core_exception(make_adapter):
    adapter = make_adapter()
    await adapter.list_metrics()
    FakeEngine.instances[-1].fail_with = ExecuteError(
        "connection refused", code="connection", hint="is the warehouse up?"
    )
    with pytest.raises(SemanticCoreException) as exc:
        await adapter.query_metrics(metrics=["revenue"])
    message = str(exc.value)
    assert "ExecuteError" in message and "connection refused" in message
    assert "is the warehouse up?" in message


@pytest.mark.parametrize("code", ["dimensions_required", "param_out_of_domain"])
def test_attribution_query_error_becomes_validation_exception(fake_binding, code):
    error = QueryError("revise the attribution request", code=code)

    with pytest.raises(SemanticValidationException) as exc_info:
        raise_mapped(error, fake_binding, requested_metrics=["revenue"])

    assert exc_info.value.payload.code == code
    assert exc_info.value.payload.metrics == ["revenue"]


def test_not_computable_error_becomes_validation_exception(fake_binding):
    error = NotComputableError(
        "the ratio denominator is nonpositive in the baseline window",
        code="denominator_nonpositive",
    )

    with pytest.raises(SemanticValidationException) as exc_info:
        raise_mapped(error, fake_binding, requested_metrics=["avg_order_value"])

    payload = exc_info.value.payload
    assert payload.code == "denominator_nonpositive"
    assert payload.metrics == ["avg_order_value"]
    assert "baseline window" in payload.message


def test_ambiguous_dimension_multiple_candidates_no_retry():
    error = QueryError(
        "ambiguous column customer_id",
        code="ambiguous_dimension",
        candidates=["orders.customer_id", "customers.customer_id"],
    )
    payload = validation_error_from_query_error(
        error, requested_metrics=["revenue"], requested_dimensions=["customer_id"]
    )
    assert payload.code == "ambiguous_dimension"
    assert payload.suggested_retry is None  # two candidates: agent must choose
    assert payload.unsupported_dimensions == ["customer_id"]
    assert "orders.customer_id" in payload.message


def test_unknown_metric_single_candidate_retry():
    error = QueryError(
        'unknown metric "revenues"',
        code="unknown_metric",
        metrics=["revenues"],
        candidates=["revenue"],
    )
    payload = validation_error_from_query_error(error, requested_metrics=["revenues"])
    assert payload.suggested_retry == {"metrics": ["revenue"]}
    assert payload.metrics == ["revenues"]


def test_engine_suggested_retry_remains_authoritative():
    suggested_retry = {
        "metrics": ["revenue"],
        "dimensions": ["customers.region"],
        "where": "orders.status = 'paid'",
    }
    error = QueryError(
        'unknown dimension "regionn"',
        code="unknown_dimension",
        candidates=["customers.region"],
        suggested_retry=suggested_retry,
    )

    payload = validation_error_from_query_error(
        error,
        requested_metrics=["revenue"],
        requested_dimensions=["regionn"],
    )

    assert payload.suggested_retry == suggested_retry


def test_non_retryable_query_error_is_core_exception(fake_binding):
    error = QueryError("planner bug", code="internal")
    with pytest.raises(SemanticCoreException):
        raise_mapped(error, fake_binding)


@pytest.mark.parametrize(
    "code",
    [
        "window_exclude_not_grouped",
        "unconformed_dimension",
        "unknown_metric_param",
        "param_expansion_too_large",
        "unknown_dataset",
        "detail_fanout",
        "metric_in_detail_query",
        # A code this adapter has never heard of. The engine keeps adding
        # them, and an unrecognized rejection still says what to revise.
        "some_future_planner_rejection",
    ],
)
def test_every_planner_rejection_is_structured(fake_binding, code):
    error = QueryError(
        "the query names something it does not group by",
        code=code,
        metrics=["area_score"],
        candidates=["metric_time"],
        hint='add "kpi_cell.merge_area_name" to group_by',
    )

    with pytest.raises(SemanticValidationException) as exc:
        raise_mapped(error, fake_binding, requested_metrics=["area_score"])

    payload = exc.value.payload
    assert payload.code == code
    assert payload.metrics == ["area_score"]
    # The engine's retry text is prose, not a machine-applicable fragment, so
    # it rides in the message — but it has to arrive.
    assert "kpi_cell.merge_area_name" in payload.message


def test_dialect_window_rejection_is_structured_for_authoring_retry(fake_binding):
    error = QueryError(
        "nth_value is unavailable for this dialect",
        code="dialect_unsupported_window_function",
        metrics=["second_revenue"],
    )
    with pytest.raises(SemanticValidationException) as exc:
        raise_mapped(error, fake_binding, requested_metrics=["second_revenue"])
    assert exc.value.payload.code == "dialect_unsupported_window_function"
    assert exc.value.payload.metrics == ["second_revenue"]


def test_model_error_is_core_exception(fake_binding):
    error = ModelError("model is invalid:", code="invalid_model")
    with pytest.raises(SemanticCoreException) as exc:
        raise_mapped(error, fake_binding)
    assert "invalid_model" in str(exc.value)


def test_unrelated_error_passes_through(fake_binding):
    with pytest.raises(RuntimeError):
        raise_mapped(RuntimeError("boom"), fake_binding)
