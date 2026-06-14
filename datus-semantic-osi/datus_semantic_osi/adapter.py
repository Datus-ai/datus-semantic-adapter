# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""DatusOSIAdapter: a BaseSemanticAdapter backed by OSI authoring + a backend.

Flow per call: load OSI YAML (source of truth) -> compile to Datus Semantic IR
-> backend lowering / validation / SQL rendering. The OSI files are the only
thing users edit; backend artifacts are generated and disposable.
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional


from datus_semantic_core import BaseSemanticAdapter
from datus_semantic_core.models import (
    DimensionInfo,
    MetricDefinition,
    QueryResult,
    ValidationIssue,
    ValidationResult,
)

from datus_semantic_osi.backend import make_backend
from datus_semantic_osi.compiler import compile_document
from datus_semantic_osi.config import DatusOSIConfig
from datus_semantic_osi.errors import OSIError, OSIValidationError
from datus_semantic_osi.ir import DatasetIR, FieldIR, MetricIR, SemanticModelIR
from datus_semantic_osi.normalizer import NormalizationResult, normalize_document
from datus_semantic_osi.profile import OSIDocument, load_osi_path
from datus_semantic_osi.validator import (
    validate_capabilities,
    validate_ir,
    validate_profile,
)


class DatusOSIAdapter(BaseSemanticAdapter):
    """OSI-native semantic adapter."""

    def __init__(self, config: DatusOSIConfig):
        super().__init__(config, service_type="osi")
        self.config = config
        self._backend = make_backend(
            config.execution_backend,
            generated_path=config.generated_path,
            db_config=config.db_config,
            datasource=config.datasource,
            timeout=config.timeout,
        )
        self._model_cache: Optional[SemanticModelIR] = None

    # ---- OSI loading / compilation -------------------------------------

    def _load_document(self) -> OSIDocument:
        return self._load_document_result().document

    def _load_document_result(self) -> NormalizationResult:
        path = self.config.semantic_models_path
        if not path or not os.path.isdir(path):
            raise OSIError(f"semantic_models_path is not a directory: {path}")
        result = normalize_document(load_osi_path(path))
        if result.errors:
            raise OSIValidationError(" ".join(result.errors))
        return result

    def _model(self) -> SemanticModelIR:
        if self._model_cache is None:
            self._model_cache = compile_document(self._load_document())
        return self._model_cache

    def _find_metric(self, name: str) -> Optional[MetricIR]:
        return next((m for m in self._model().metrics if m.name == name), None)

    def _dataset_by_name(self) -> Dict[str, DatasetIR]:
        return {dataset.name: dataset for dataset in self._model().datasets}

    def _root_dataset_names_for_metric(
        self, metric: MetricIR, seen_metrics: Optional[set[str]] = None
    ) -> List[str]:
        if metric.dataset:
            return [metric.dataset]

        seen_metrics = seen_metrics or set()
        if metric.name in seen_metrics:
            return []
        seen_metrics.add(metric.name)

        dataset_names: List[str] = []
        for input_metric in metric.inputs:
            referenced = self._find_metric(input_metric.name)
            if referenced is None:
                continue
            dataset_names.extend(
                self._root_dataset_names_for_metric(referenced, seen_metrics)
            )

        if dataset_names:
            return self._dedupe(dataset_names)

        if metric.measures and self._model().datasets:
            # MetricFlow lowering places dataset-less backing measures on the
            # first declared dataset, so discovery follows the same fallback.
            return [self._model().datasets[0].name]

        return []

    @staticmethod
    def _dedupe(values: List[str]) -> List[str]:
        seen: set[str] = set()
        result: List[str] = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result

    def _metric_metadata(self, metric: MetricIR) -> Dict[str, Any]:
        dataset_names = self._root_dataset_names_for_metric(metric)
        metadata: Dict[str, Any] = {
            "dataset": metric.dataset
            or (dataset_names[0] if len(dataset_names) == 1 else None),
            "datasets": dataset_names if len(dataset_names) > 1 else None,
            "time_dimension": metric.time_dimension,
            "metric_kind": metric.kind.value,
        }

        if metric.expression:
            metadata["expr"] = metric.expression

        if metric.inputs:
            inputs = []
            for input_metric in metric.inputs:
                item: Dict[str, Any] = {"name": input_metric.name}
                if input_metric.alias:
                    item["alias"] = input_metric.alias
                if input_metric.offset_window:
                    item["offset_window"] = input_metric.offset_window
                inputs.append(item)
            metadata["inputs"] = inputs
            offset_window = next(
                (
                    item.get("offset_window")
                    for item in inputs
                    if item.get("offset_window")
                ),
                None,
            )
            if offset_window:
                metadata["offset_window"] = offset_window
        elif metric.offset_window:
            metadata["offset_window"] = metric.offset_window

        if metric.window:
            metadata["window"] = metric.window
        if metric.grain_to_date:
            metadata["grain_to_date"] = metric.grain_to_date
        if metric.numerator:
            metadata["numerator"] = metric.numerator
        if metric.denominator:
            metadata["denominator"] = metric.denominator
        if metric.measures:
            metadata["measure"] = metric.measures[0].name

        metadata.update(metric.metadata)
        return {key: value for key, value in metadata.items() if value is not None}

    # ---- BaseSemanticAdapter interface ---------------------------------

    async def list_metrics(
        self, path: Optional[List[str]] = None, limit: int = 100, offset: int = 0
    ) -> List[MetricDefinition]:
        metrics = self._model().metrics
        if path:
            metrics = [
                m
                for m in metrics
                if isinstance(m.metadata.get("subject_path"), list)
                and m.metadata["subject_path"][: len(path)] == path
            ]
        metrics = metrics[offset : offset + limit]
        return [
            MetricDefinition(
                name=m.name,
                description=m.description or None,
                type=m.kind.value,
                dimensions=[d.name for d in self._dimensions_for_metric(m)],
                measures=[x.name for x in m.measures],
                unit=m.unit,
                format=m.format,
                path=m.metadata.get("subject_path")
                if isinstance(m.metadata.get("subject_path"), list)
                else None,
                metadata=self._metric_metadata(m),
            )
            for m in metrics
        ]

    @staticmethod
    def _dimension_info(field: FieldIR, name: Optional[str] = None) -> DimensionInfo:
        return DimensionInfo(
            name=name or field.name,
            description=field.description or None,
            type="time" if field.type == "time" else field.type,
            is_primary_key=False,
        )

    @staticmethod
    def _relationship_join_name(to_dataset: DatasetIR, fallback_identifier: str) -> str:
        primary = next((i for i in to_dataset.identifiers if i.type == "primary"), None)
        return primary.name if primary else fallback_identifier

    def _dimensions_for_dataset(
        self,
        dataset_name: str,
        prefix: Optional[List[str]] = None,
        visited: Optional[set[str]] = None,
    ) -> List[DimensionInfo]:
        datasets = self._dataset_by_name()
        dataset = datasets.get(dataset_name)
        if dataset is None:
            return []

        prefix = prefix or []
        visited = visited or set()
        visited.add(dataset_name)

        dimensions = [
            self._dimension_info(
                field,
                "__".join([*prefix, field.name]) if prefix else field.name,
            )
            for field in dataset.fields
        ]

        for relationship in self._model().relationships:
            if relationship.from_dataset != dataset_name:
                continue
            if relationship.to_dataset in visited:
                continue
            to_dataset = datasets.get(relationship.to_dataset)
            if to_dataset is None:
                continue
            join_name = self._relationship_join_name(
                to_dataset, relationship.to_identifier
            )
            dimensions.extend(
                self._dimensions_for_dataset(
                    relationship.to_dataset,
                    prefix=[*prefix, join_name],
                    visited=set(visited),
                )
            )
        return dimensions

    def _dimensions_for_metric(self, metric: MetricIR) -> List[DimensionInfo]:
        dimensions: List[DimensionInfo] = []
        seen: set[str] = set()
        for dataset_name in self._root_dataset_names_for_metric(metric):
            for dimension in self._dimensions_for_dataset(dataset_name):
                if dimension.name in seen:
                    continue
                seen.add(dimension.name)
                dimensions.append(dimension)
        return dimensions

    async def get_dimensions(
        self, metric_name: str, path: Optional[List[str]] = None
    ) -> List[DimensionInfo]:
        metric = self._find_metric(metric_name)
        if metric is None:
            return []
        return self._dimensions_for_metric(metric)

    async def query_metrics(
        self,
        metrics: List[str],
        dimensions: Optional[List[str]] = None,
        path: Optional[List[str]] = None,
        time_start: Optional[str] = None,
        time_end: Optional[str] = None,
        time_granularity: Optional[str] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        order_by: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> QueryResult:
        model = self._model()
        live = getattr(self._backend, "has_live_connection", False)

        if dry_run and not live:
            sql = self._backend.render_sql(
                model,
                metrics=metrics,
                dimensions=dimensions,
                time_start=time_start,
                time_end=time_end,
                where=where,
                limit=limit,
            )
            return QueryResult(
                columns=["sql"],
                data=[{"sql": sql}],
                metadata={"explain": True, "sql": sql},
            )

        if not live:
            raise NotImplementedError(
                "Live query execution requires a configured db_config so the backend can "
                "delegate to its warehouse connection. Use dry_run=True for the plan."
            )

        # delegate live execution / explain to the wrapped MetricFlowAdapter
        executor = self._backend.make_executor(model)
        return await executor.query_metrics(
            metrics,
            dimensions=dimensions or [],
            path=path,
            time_start=time_start,
            time_end=time_end,
            time_granularity=time_granularity,
            where=where,
            limit=limit,
            order_by=order_by,
            dry_run=dry_run,
        )

    async def validate_semantic(self, scope: str = "all") -> ValidationResult:
        issues: List[ValidationIssue] = []

        # Stage 1: OSI Profile validation (authoring level).
        try:
            normalization = self._load_document_result()
            doc = normalization.document
        except OSIError as e:
            return ValidationResult(
                valid=False, issues=[ValidationIssue(severity="error", message=str(e))]
            )
        issues.extend(
            ValidationIssue(severity="warning", message=m)
            for m in [*normalization.warnings, *normalization.actions]
        )
        issues.extend(
            ValidationIssue(severity="error", message=m) for m in validate_profile(doc)
        )

        # Stage 2: compile to IR (business-semantic errors fail fast).
        try:
            model = compile_document(doc)
        except OSIValidationError as e:
            issues.append(ValidationIssue(severity="error", message=str(e)))
            return ValidationResult(valid=False, issues=issues)

        # Stage 3: IR + backend capability validation.
        issues.extend(
            ValidationIssue(severity="error", message=m) for m in validate_ir(model)
        )
        caps = getattr(self._backend, "capabilities", {}) or {}
        issues.extend(
            ValidationIssue(severity="error", message=m)
            for m in validate_capabilities(model, caps)
        )
        if any(i.severity == "error" for i in issues):
            return ValidationResult(valid=False, issues=issues)

        # Stage 4: backend validation. With a live connection, delegate to
        # MetricFlowAdapter for the full pipeline (lint + parse + semantic +
        # data-warehouse validation); otherwise run parse + semantic only.
        if getattr(self._backend, "has_live_connection", False):
            executor = self._backend.make_executor(model)
            backend_result = await executor.validate_semantic(scope=scope)
        else:
            backend_result = self._backend.validate(model)
        issues.extend(backend_result.issues)
        valid = backend_result.valid and not any(i.severity == "error" for i in issues)
        return ValidationResult(valid=valid, issues=issues)
