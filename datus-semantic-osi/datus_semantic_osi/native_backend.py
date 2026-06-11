# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""DatusNativeBackend: lower Datus Semantic IR directly to SQL.

A first-version native backend that generates SQL without MetricFlow. It covers
the restricted subset from the design doc: simple aggregations, expression and
ratio metrics, filtered datasets, runtime ``where``, group-by dimensions, and a
time-range constraint. Cumulative / derived / offset metrics, fanout correction,
date spines, SCD, and arbitrary window functions are intentionally out of scope.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional

from datus_semantic_core.models import ValidationIssue, ValidationResult

from datus_semantic_osi.backend import SemanticExecutionBackend
from datus_semantic_osi.errors import OSIValidationError
from datus_semantic_osi.ir import (
    Aggregation,
    DatasetIR,
    FilterScope,
    MeasureIR,
    MetricIR,
    MetricKind,
    SemanticModelIR,
)
from datus_semantic_osi.validator import validate_capabilities, validate_ir

_AGG_SQL = {
    Aggregation.SUM: "SUM({expr})",
    Aggregation.COUNT: "COUNT({expr})",
    Aggregation.COUNT_DISTINCT: "COUNT(DISTINCT {expr})",
    Aggregation.AVERAGE: "AVG({expr})",
    Aggregation.MIN: "MIN({expr})",
    Aggregation.MAX: "MAX({expr})",
}


def _measure_sql(measure: MeasureIR) -> str:
    return _AGG_SQL[measure.agg].format(expr=measure.expr)


class DatusNativeBackend(SemanticExecutionBackend):
    """IR -> SQL backend (no MetricFlow)."""

    name = "native"
    capabilities = {
        "metric_kinds": ["aggregate", "expression", "ratio"],
        "filtered_dataset": True,
        "runtime_where": True,
        "many_to_one_join": True,
        "time_bucket": ["day", "week", "month", "quarter", "year"],
        "dry_run": "sql",
        "artifact": "sql",
    }

    def lower(self, model: SemanticModelIR) -> Dict[str, str]:
        """Lower the IR into one SQL statement per metric (single-metric form)."""
        return {m.name: self.render_sql(model, [m.name]) for m in model.metrics}

    def validate(self, model: SemanticModelIR) -> ValidationResult:
        issues: List[ValidationIssue] = [
            ValidationIssue(severity="error", message=m) for m in validate_ir(model)
        ]
        issues.extend(
            ValidationIssue(severity="error", message=m)
            for m in validate_capabilities(model, self.capabilities)
        )
        return ValidationResult(
            valid=not any(i.severity == "error" for i in issues), issues=issues
        )

    # ---- SQL generation ------------------------------------------------

    def _metric_select_expr(self, metric: MetricIR) -> str:
        measures = {m.name: m for m in metric.measures}
        if metric.kind is MetricKind.AGGREGATE:
            return _measure_sql(metric.measures[0])
        if metric.kind is MetricKind.RATIO:
            num = _measure_sql(measures[metric.numerator])
            den = _measure_sql(measures[metric.denominator])
            return f"{num} / NULLIF({den}, 0)"
        if metric.kind is MetricKind.EXPRESSION:
            expr = metric.expression
            for name, measure in measures.items():
                expr = re.sub(rf"\b{re.escape(name)}\b", _measure_sql(measure), expr)
            return expr
        raise OSIValidationError(
            f"metric kind `{metric.kind.value}` is not supported by the native backend.",
            metric=metric.name,
        )

    def _dataset_where(self, ds: DatasetIR) -> List[str]:
        return [f.expression for f in ds.filters if f.scope is FilterScope.DATASET]

    def render_sql(
        self,
        model: SemanticModelIR,
        metrics: List[str],
        dimensions: Optional[List[str]] = None,
        time_start: Optional[str] = None,
        time_end: Optional[str] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> str:
        metric_objs = [
            next(m for m in model.metrics if m.name == name) for name in metrics
        ]
        datasets = {m.dataset for m in metric_objs}
        if len(datasets) != 1:
            raise OSIValidationError(
                "native backend renders one dataset per query; "
                f"metrics span datasets {sorted(datasets)}."
            )
        ds = next(d for d in model.datasets if d.name == metric_objs[0].dataset)

        select_parts = list(dimensions or [])
        for metric in metric_objs:
            select_parts.append(f"{self._metric_select_expr(metric)} AS {metric.name}")

        where_parts = self._dataset_where(ds)
        if where:
            where_parts.append(where)
        if time_start and ds.primary_time_dimension:
            where_parts.append(f"{ds.primary_time_dimension} >= '{time_start}'")
        if time_end and ds.primary_time_dimension:
            where_parts.append(f"{ds.primary_time_dimension} <= '{time_end}'")

        source = ds.sql_table or (f"({ds.sql_query})" if ds.sql_query else None)
        if source is None:
            raise OSIValidationError(
                f"dataset `{ds.name}` has no source.", metric=metrics[0]
            )

        sql = f"SELECT {', '.join(select_parts)} FROM {source}"
        if where_parts:
            sql += " WHERE " + " AND ".join(f"({p})" for p in where_parts)
        if dimensions:
            sql += " GROUP BY " + ", ".join(dimensions)
        if limit:
            sql += f" LIMIT {limit}"
        return sql
