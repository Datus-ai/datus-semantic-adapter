# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Datus OSI validation: Profile rules, IR rules, and backend capability checks.

All issues are phrased as business semantics, never backend syntax. ``ensure_valid``
turns a list of issues into an :class:`OSIValidationError`.
"""

from __future__ import annotations

from typing import List

from datus_semantic_osi.errors import OSIValidationError
from datus_semantic_osi.ir import MetricKind, SemanticModelIR
from datus_semantic_osi.normalizer import normalization_errors
from datus_semantic_osi.profile import OSIDocument


def validate_profile(doc: OSIDocument) -> List[str]:
    """Validate the executable OSI Profile subset (authoring-level rules)."""
    issues: List[str] = []
    dataset_names = {d.name for d in doc.datasets}

    if not doc.datasets:
        issues.append("Document declares no datasets; at least one is required.")

    for ds in doc.datasets:
        if not (ds.source and (ds.source.table or ds.source.query)):
            issues.append(f"Dataset `{ds.name}` has no source table or query.")
        field_names = {dim.name for dim in ds.dimensions}
        if ds.time_dimension:
            field_names.add(ds.time_dimension.name)

    for metric in doc.metrics:
        if metric.dataset and metric.dataset not in dataset_names:
            issues.append(
                f"Metric `{metric.name}` references dataset `{metric.dataset}` which is not declared."
            )
        has_expr = bool(metric.expression)
        has_ratio = (
            (metric.metric_kind == "ratio")
            and bool(metric.numerator)
            and bool(metric.denominator)
        )
        if not has_expr and not has_ratio:
            issues.append(
                f"Metric `{metric.name}` needs an `expression` or explicit ratio numerator/denominator."
            )
        # time_dimension must be declared on the metric's dataset
        if metric.time_dimension and metric.dataset in dataset_names:
            ds = next(d for d in doc.datasets if d.name == metric.dataset)
            declared = {dim.name for dim in ds.dimensions}
            if ds.time_dimension:
                declared.add(ds.time_dimension.name)
            if metric.time_dimension not in declared:
                issues.append(
                    f"Metric `{metric.name}` uses time dimension `{metric.time_dimension}` "
                    f"not declared on dataset `{metric.dataset}`."
                )
    issues.extend(normalization_errors(doc))
    return issues


def validate_ir(model: SemanticModelIR) -> List[str]:
    """Validate the compiled IR (structural / semantic invariants)."""
    issues: List[str] = []
    dataset_names = {d.name for d in model.datasets}

    # Dataset names must be unique across the merged model. The same physical
    # table may back many datasets, but each must have a distinct name (it
    # becomes a separate backend data source); a duplicate name would emit two
    # conflicting data sources and collide their namespaced measures.
    seen_datasets: dict = {}
    for ds in model.datasets:
        seen_datasets[ds.name] = seen_datasets.get(ds.name, 0) + 1
    for name, count in seen_datasets.items():
        if count > 1:
            issues.append(
                f"Dataset name `{name}` is declared {count} times; dataset names must be "
                f"unique across the model (rename one, or merge them)."
            )

    # Metric names must be unique across the merged model (each lowers to one
    # backend metric); a duplicate name would emit two conflicting metrics.
    seen_metrics: dict = {}
    for metric in model.metrics:
        seen_metrics[metric.name] = seen_metrics.get(metric.name, 0) + 1
    for name, count in seen_metrics.items():
        if count > 1:
            issues.append(
                f"Metric name `{name}` is declared {count} times; metric names must be "
                f"unique across the model (rename one)."
            )

    measure_owner: dict = {}
    for metric in model.metrics:
        if metric.dataset and metric.dataset not in dataset_names:
            issues.append(
                f"Metric `{metric.name}` references dataset `{metric.dataset}` which is not declared."
            )
        if metric.kind is MetricKind.AGGREGATE and len(metric.measures) != 1:
            issues.append(
                f"Aggregate metric `{metric.name}` must have exactly one backing measure."
            )
        if metric.kind is MetricKind.RATIO and not (
            metric.numerator and metric.denominator
        ):
            issues.append(
                f"Ratio metric `{metric.name}` must declare both numerator and denominator."
            )
        if metric.kind is MetricKind.EXPRESSION and not metric.expression:
            issues.append(
                f"Expression metric `{metric.name}` must carry an expression."
            )
        if metric.kind is MetricKind.CUMULATIVE and not (
            metric.window or metric.grain_to_date
        ):
            issues.append(
                f"Cumulative metric `{metric.name}` must declare a window or grain_to_date."
            )
        for measure in metric.measures:
            owner = measure_owner.get(measure.name)
            if owner is not None and owner != metric.dataset:
                issues.append(
                    f"Measure `{measure.name}` appears in multiple datasets "
                    f"(`{owner}` and `{metric.dataset}`); measure names must be globally unique."
                )
            measure_owner[measure.name] = metric.dataset

    for rel in model.relationships:
        if rel.from_dataset not in dataset_names:
            issues.append(
                f"Relationship `{rel.name}` from unknown dataset `{rel.from_dataset}`."
            )
        if rel.to_dataset not in dataset_names:
            issues.append(
                f"Relationship `{rel.name}` to unknown dataset `{rel.to_dataset}`."
            )

    # An element name must have one consistent type across the whole model:
    # MetricFlow rejects e.g. `start_date` being a time dimension in one data
    # source and categorical in another, or a column being an identifier in one
    # and a dimension in another. Relationship-derived foreign identifiers are
    # accounted for so a join key declared as a dimension on the fact side is the
    # conflict that gets reported.
    rel_fk = {(r.from_dataset, r.from_identifier) for r in model.relationships}
    rel_fk_names = {r.from_identifier for r in model.relationships} | {
        r.to_identifier for r in model.relationships
    }
    element_type: dict = {}
    for ds in model.datasets:
        for ident in ds.identifiers:
            element_type.setdefault(ident.name, set()).add("identifier")
        for field in ds.fields:
            # a field that is the join key on this dataset becomes an identifier
            if field.name in rel_fk_names or (ds.name, field.name) in rel_fk:
                element_type.setdefault(field.name, set()).add("identifier")
            elif field.type == "time":
                element_type.setdefault(field.name, set()).add("time")
            else:
                element_type.setdefault(field.name, set()).add("dimension")
    for name, types in element_type.items():
        if len(types) > 1:
            issues.append(
                f"Element `{name}` is used as multiple types {sorted(types)} across datasets; "
                f"a column must have one consistent type (identifier / time / dimension) model-wide."
            )
    return issues


def validate_capabilities(model: SemanticModelIR, capabilities: dict) -> List[str]:
    """Check the IR against a backend's declared capabilities before lowering."""
    issues: List[str] = []
    supported_kinds = set(capabilities.get("metric_kinds", []))
    for metric in model.metrics:
        if supported_kinds and metric.kind.value not in supported_kinds:
            issues.append(
                f"Backend does not support metric kind `{metric.kind.value}` "
                f"used by metric `{metric.name}`. Supported: {sorted(supported_kinds)}."
            )
    return issues


def ensure_valid(issues: List[str]) -> None:
    """Raise an OSIValidationError if there are any issues."""
    if issues:
        raise OSIValidationError(" ".join(issues))
