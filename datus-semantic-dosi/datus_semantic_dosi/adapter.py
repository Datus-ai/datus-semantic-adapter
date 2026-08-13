# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""The Dosi semantic adapter: a thin translator onto the native engine.

All planning, SQL generation, and execution happen inside the Rust engine;
this class maps the Datus contract onto the engine API. Engine calls are
synchronous and GIL-releasing, so they run under ``asyncio.to_thread``.

Scope note: an engine instance serves ONE OSI model file, so the ``path``
(subject-tree) arguments are accepted and ignored.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

import yaml
from datus_semantic_core.authoring import MetricMutationResult, MetricSource
from datus_semantic_core.base import BaseSemanticAdapter
from datus_semantic_core.exceptions import SemanticCoreException
from datus_semantic_core.models import (
    DimensionInfo,
    MetricDefinition,
    QueryResult,
    SemanticModelInfo,
    SemanticValidationError,
    ValidationIssue,
    ValidationResult,
)

from datus_semantic_dosi.authoring import dosi_validation_text_payload
from datus_semantic_dosi.config import DosiConfig
from datus_semantic_dosi.dialects import resolve_engine_dialect
from datus_semantic_dosi.engine import (
    EngineHandle,
    datus_extension_version,
    load_binding,
)
from datus_semantic_dosi.errors import (
    SemanticValidationException,
    raise_mapped,
)
from datus_semantic_dosi.model import (
    load_document,
    metric_payloads,
    queryable_grains,
)


class DosiAdapter(BaseSemanticAdapter):
    """Datus semantic adapter backed by the native Rust Dosi engine."""

    def __init__(self, config: DosiConfig):
        super().__init__(config, service_type="dosi")
        self.config: DosiConfig = config
        self._handle = EngineHandle(config)

    # ==================== Semantic Model Interface ====================

    def list_semantic_models(
        self,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
    ) -> List[SemanticModelInfo]:
        return [self._model_info(row) for row in self._engine().datasets()]

    def get_semantic_model(
        self,
        table_name: str,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
    ) -> Optional[SemanticModelInfo]:
        for row in self._engine().datasets():
            source = str(row.get("source", ""))
            if table_name in (row.get("name"), source) or source.endswith(
                f".{table_name}"
            ):
                return self._model_info(row)
        return None

    # ==================== Metrics Interface ====================

    async def list_metrics(
        self,
        path: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[MetricDefinition]:
        extension_version = await asyncio.to_thread(datus_extension_version)
        engine = await asyncio.to_thread(self._engine)
        rows = await asyncio.to_thread(engine.metrics)
        dataset_rows = await asyncio.to_thread(engine.datasets)
        dimension_rows = await asyncio.to_thread(engine.dimensions)
        payloads = self._metric_payloads()
        binding = await asyncio.to_thread(load_binding)
        metrics = []
        for row in rows:
            hints = payloads.get(str(row.get("name") or ""), {})
            window = hints.get("window")
            effective_time = self._effective_time_dimension(
                row, dataset_rows, dimension_rows
            )
            metadata: Dict[str, Any] = {
                "datasets": list(row.get("datasets") or []),
                "base_kind": row.get("kind"),
                "datus_ext_version": extension_version,
            }
            for key in ("window_family", "window_function"):
                if row.get(key):
                    metadata[key] = row[key]
            if effective_time:
                metadata["time_dimension"] = effective_time
            if isinstance(window, dict):
                requires_time_axis = await asyncio.to_thread(
                    self._probe_requires_time_axis,
                    engine,
                    binding,
                    str(row.get("name") or ""),
                )
                metadata.update(
                    {
                        "window": window,
                        "requires_time_axis": requires_time_axis,
                    }
                )
            if "fill_nulls_with" in hints:
                metadata["fill_nulls_with"] = hints["fill_nulls_with"]
            metrics.append(
                MetricDefinition(
                    name=row["name"],
                    description=row.get("description") or None,
                    type="window" if isinstance(window, dict) else row.get("kind"),
                    # The native catalog currently exposes model-wide dimensions,
                    # not the dimensions queryable for this specific metric. Do
                    # not publish that broader set as metric-level capability;
                    # callers use get_dimensions(), which verifies each candidate
                    # through the native planner.
                    dimensions=[],
                    measures=list(row.get("measures") or []),
                    unit=str(hints.get("unit"))
                    if hints.get("unit") is not None
                    else None,
                    format=(
                        str(hints.get("format"))
                        if hints.get("format") is not None
                        else None
                    ),
                    path=(
                        [str(item) for item in hints.get("subject_path")]
                        if isinstance(hints.get("subject_path"), list)
                        else None
                    ),
                    metadata=metadata,
                )
            )
        return metrics[offset : offset + limit]

    async def get_dimensions(
        self,
        metric_name: str,
        path: Optional[List[str]] = None,
    ) -> List[DimensionInfo]:
        engine = await asyncio.to_thread(self._engine)
        metric_rows = await asyncio.to_thread(engine.metrics)
        metric_names = [m["name"] for m in metric_rows]
        if metric_name not in metric_names:
            raise SemanticValidationException(
                SemanticValidationError(
                    code="unknown_metric",
                    metrics=[metric_name],
                    message=(
                        f"unknown metric {metric_name!r} | "
                        f"candidates: {', '.join(metric_names)}"
                    ),
                )
            )
        # Expose Dosi's canonical discovery names unchanged. Query inputs are
        # deliberately not constrained to this list: Dosi itself accepts a
        # globally unique bare field name and reports structured ambiguity.
        metric_row = next(row for row in metric_rows if row.get("name") == metric_name)
        dataset_rows = await asyncio.to_thread(engine.datasets)
        binding = await asyncio.to_thread(load_binding)
        rows = await asyncio.to_thread(engine.dimensions)
        connection = self._handle.profile_name
        dialect = self._dry_run_dialect(binding, connection)

        requires_time_axis = await asyncio.to_thread(
            self._probe_requires_time_axis,
            engine,
            binding,
            metric_name,
        )
        primary_time_dimension = self._effective_time_dimension(
            metric_row, dataset_rows, rows
        )

        def _queryable_rows() -> List[tuple[Dict[str, Any], List[str]]]:
            queryable: List[tuple[Dict[str, Any], List[str]]] = []
            axis_grains: List[str] = []
            axis_error: Any = None
            if requires_time_axis:
                for grain in queryable_grains(
                    next(
                        (
                            row.get("time_granularity")
                            for row in rows
                            if row.get("name") == primary_time_dimension
                        ),
                        None,
                    )
                ):
                    error = self._probe_compile(
                        engine,
                        binding,
                        metric_name,
                        [{"field": "metric_time", "grain": grain}],
                        dialect=dialect,
                        connection=connection,
                    )
                    if error is None:
                        axis_grains.append(grain)
                    elif axis_error is None:
                        axis_error = error
                if not axis_grains and axis_error is not None:
                    raise_mapped(
                        axis_error,
                        binding,
                        requested_metrics=[metric_name],
                        requested_dimensions=["metric_time"],
                    )

            for row in rows:
                name = str(row.get("name") or "")
                if not name:
                    continue
                if row.get("is_time"):
                    if requires_time_axis and name == primary_time_dimension:
                        grains = list(axis_grains)
                    else:
                        grains = []
                        for grain in queryable_grains(row.get("time_granularity")):
                            group_by = [{"field": name, "grain": grain}]
                            if requires_time_axis:
                                group_by.append(
                                    {
                                        "field": "metric_time",
                                        "grain": axis_grains[0],
                                    }
                                )
                            error = self._probe_compile(
                                engine,
                                binding,
                                metric_name,
                                group_by,
                                dialect=dialect,
                                connection=connection,
                            )
                            if error is None:
                                grains.append(grain)
                    if grains:
                        queryable.append((row, grains))
                    continue

                group_by = [{"field": name}]
                if requires_time_axis:
                    group_by.insert(
                        0,
                        {"field": "metric_time", "grain": axis_grains[0]},
                    )
                error = self._probe_compile(
                    engine,
                    binding,
                    metric_name,
                    group_by,
                    dialect=dialect,
                    connection=connection,
                )
                if error is None:
                    queryable.append((row, []))
            return queryable

        queryable_rows = await asyncio.to_thread(_queryable_rows)
        return [
            DimensionInfo(
                name=str(row.get("name") or ""),
                description=row.get("description") or None,
                type="time" if row.get("is_time") else None,
                is_primary_time=bool(
                    primary_time_dimension and row.get("name") == primary_time_dimension
                ),
                time_granularities=(
                    grains
                    if primary_time_dimension
                    and row.get("name") == primary_time_dimension
                    else []
                ),
            )
            for row, grains in queryable_rows
            if row.get("name")
        ]

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
        binding = await asyncio.to_thread(load_binding)
        engine = await asyncio.to_thread(self._engine)
        dimensions = list(dimensions or [])
        # Fetched off the event loop; _build_query stays pure (no engine I/O).
        dimension_rows = await asyncio.to_thread(engine.dimensions)
        metric_rows = await asyncio.to_thread(engine.metrics)

        query = self._build_query(
            dimension_rows,
            metric_rows,
            metrics=metrics,
            dimensions=dimensions,
            time_start=time_start,
            time_end=time_end,
            time_granularity=time_granularity,
            where=where,
            limit=limit,
            order_by=order_by,
        )
        connection = self._handle.profile_name
        try:
            if dry_run:
                compiled = await asyncio.to_thread(
                    engine.compile,
                    query,
                    dialect=self._dry_run_dialect(binding, connection),
                    connection=connection,
                    # Dry-run SQL is read by humans and LLM callers, never fed
                    # to a driver, so hand back the formatted form.
                    pretty=True,
                )
                return QueryResult(
                    columns=["sql"],
                    data=[{"sql": compiled["sql"]}],
                    metadata={
                        "sql": compiled["sql"],
                        "dialect": compiled["dialect"],
                        "dry_run": True,
                        "explain": True,
                    },
                )
            result = await asyncio.to_thread(
                engine.execute,
                query,
                connection=connection,
                timeout_secs=float(self.config.timeout_seconds),
            )
            return QueryResult(
                columns=list(result["columns"]),
                data=list(result["rows"]),
                metadata={
                    "sql": result["sql"],
                    "dialect": result["dialect"],
                    "row_count": result["row_count"],
                },
            )
        except Exception as exc:  # noqa: BLE001 - mapped to typed errors below
            # A SemanticValidationException (raised only by _build_query, before
            # this try) is not an engine error; raise_mapped re-raises it as-is.
            raise_mapped(
                exc,
                binding,
                requested_metrics=metrics,
                requested_dimensions=dimensions,
                requested_time_granularity=time_granularity,
            )
            raise  # unreachable; raise_mapped always raises

    async def validate_semantic(
        self,
        scope: str = "all",
        semantic_model_name: str = "",
    ) -> ValidationResult:
        """Validate the configured model, optionally asserting its model name.

        Dosi v1 compiles exactly one semantic model per document, so validating
        that document is also a targeted validation once the requested name is
        confirmed. The optional name keeps the adapter compatible with Datus
        authoring publish gates without pretending to support multi-model files.
        """
        try:
            model_path = self._handle.model_file()
        except SemanticCoreException as exc:
            return ValidationResult(
                valid=False,
                issues=[ValidationIssue(severity="error", message=str(exc))],
            )

        def _validate() -> dict[str, Any]:
            with open(model_path, encoding="utf-8") as fh:
                model_text = fh.read()
            payload = dosi_validation_text_payload(model_text)
            if payload.get("valid") and semantic_model_name:
                document = yaml.safe_load(model_text) or {}
                names = (
                    [
                        str(model.get("name") or "")
                        for model in document.get("semantic_model", [])
                        if isinstance(model, dict)
                    ]
                    if isinstance(document, dict)
                    else []
                )
                if semantic_model_name not in names:
                    payload["valid"] = False
                    payload.setdefault("issues", []).append(
                        {
                            "severity": "error",
                            "code": "semantic_model_not_found",
                            "location": "semantic_model",
                            "message": (
                                f"semantic model {semantic_model_name!r} "
                                "was not found; "
                                f"available: {', '.join(names) or '(none)'}"
                            ),
                        }
                    )
            return payload

        try:
            payload = await asyncio.to_thread(_validate)
        except OSError as exc:
            return ValidationResult(
                valid=False,
                issues=[
                    ValidationIssue(
                        severity="error",
                        message=f"cannot read semantic model {model_path!r}: {exc}",
                    )
                ],
            )

        issues = [
            ValidationIssue(
                severity=issue.get("severity") or "error",
                message=f"{issue.get('code', 'issue')}: {issue.get('message', '')}",
                location=issue.get("location") or None,
            )
            for issue in payload.get("issues", [])
        ]
        for err in payload.get("compile_errors", []):
            message = f"{err.get('code', 'compile_error')}: {err.get('message', '')}"
            if err.get("hint"):
                message = f"{message} | {err['hint']}"
            issues.append(
                ValidationIssue(
                    severity="error",
                    message=message,
                    location=err.get("location") or None,
                )
            )
        return ValidationResult(valid=bool(payload.get("valid")), issues=issues)

    # ==================== Authoring Interface ====================
    # Backend/editor surface; not an agent/LLM tool. Dosi authors the same
    # OSI YAML files as the osi adapter, so the file read/write/validate logic is
    # reused from the OSI adapter — only the execution/query engine differs
    # (native Rust here vs the Python compiler there). This keeps direct adapter
    # mutations and Datus-agent authoring on the same strict OSI schema contract.

    def _authoring_root(self) -> str:
        """The OSI model path authoring operates on (mirrors resolve_model_file).

        An explicit ``semantic_model_path`` pins authoring to exactly that file
        so sibling models in the same directory are never touched; otherwise the
        configured ``semantic_models_path`` directory is scanned.
        """
        if self.config.semantic_model_path:
            return self.config.semantic_model_path
        if self.config.semantic_models_path:
            return self.config.semantic_models_path
        raise SemanticCoreException(
            "dosi authoring requires semantic_model_path or semantic_models_path"
        )

    def _author(self) -> Any:
        from datus_semantic_dosi.authoring import DosiMetricAuthor

        return DosiMetricAuthor(self._authoring_root())

    def read_metric_source(
        self,
        metric_name: str,
        *,
        subject_path: Optional[List[str]] = None,
    ) -> MetricSource:
        return self._author().read(metric_name)

    def write_metric_source(
        self,
        metric_name: str,
        source: str,
        *,
        subject_path: Optional[List[str]] = None,
        create: bool = False,
    ) -> MetricMutationResult:
        return self._author().write(
            metric_name, source, subject_path=subject_path, create=create
        )

    def delete_metric_source(
        self,
        metric_name: str,
        *,
        subject_path: Optional[List[str]] = None,
    ) -> MetricMutationResult:
        return self._author().delete(metric_name)

    def validate_metric_source(
        self,
        source: str,
        *,
        metric_name: Optional[str] = None,
    ) -> ValidationResult:
        return self._author().validate(source, metric_name=metric_name)

    # ==================== Internals ====================

    def _engine(self) -> Any:
        return self._handle.get()

    def _model_info(self, row: Dict[str, Any]) -> SemanticModelInfo:
        return SemanticModelInfo(
            name=str(row.get("name", "")),
            table_name=str(row.get("source", "")),
            platform_type="dosi",
            extra={k: v for k, v in row.items() if k not in ("name", "source")},
        )

    def _metric_payloads(self) -> Dict[str, Dict[str, Any]]:
        """Presentation metadata from the raw model; Dosi still validates it."""

        return metric_payloads(load_document(self._handle.model_file()))

    @staticmethod
    def _probe_compile(
        engine: Any,
        binding: Any,
        metric_name: str,
        group_by: List[Dict[str, Any]],
        *,
        dialect: Optional[str],
        connection: Optional[str],
    ) -> Any:
        """Compile one discovery query and return only planner rejections."""

        try:
            engine.compile(
                {"metrics": [metric_name], "group_by": group_by},
                dialect=dialect,
                connection=connection,
            )
        except binding.QueryError as exc:
            return exc
        except Exception as exc:
            raise_mapped(
                exc,
                binding,
                requested_metrics=[metric_name],
                requested_dimensions=[
                    str(item.get("field") or "") for item in group_by
                ],
            )
        return None

    @classmethod
    def _probe_requires_time_axis(
        cls,
        engine: Any,
        binding: Any,
        metric_name: str,
    ) -> bool:
        """Ask the native planner whether a metric needs a grouped time axis."""

        without_axis = cls._probe_compile(
            engine,
            binding,
            metric_name,
            [],
            dialect="duckdb",
            connection=None,
        )
        if without_axis is None:
            return False
        for grain in queryable_grains(None):
            if (
                cls._probe_compile(
                    engine,
                    binding,
                    metric_name,
                    [{"field": "metric_time", "grain": grain}],
                    dialect="duckdb",
                    connection=None,
                )
                is None
            ):
                return True
        return str(getattr(without_axis, "code", "")) in {
            "no_primary_time_dimension",
            "unknown_dimension",
        }

    @staticmethod
    def _effective_time_dimension(
        metric_row: Dict[str, Any],
        dataset_rows: List[Dict[str, Any]],
        dimension_rows: List[Dict[str, Any]],
    ) -> str:
        """Discover time dimensions from the engine's compiled catalog rows."""

        dimension_names = {
            str(row.get("name") or "") for row in dimension_rows if row.get("name")
        }
        explicit = str(metric_row.get("time_dimension") or "").strip()
        if explicit:
            if explicit in dimension_names:
                return explicit
            metric_datasets = [
                str(name) for name in metric_row.get("datasets") or [] if name
            ]
            qualified = [
                f"{dataset}.{explicit}"
                for dataset in metric_datasets
                if f"{dataset}.{explicit}" in dimension_names
            ]
            if len(qualified) == 1:
                return qualified[0]

        metric_datasets = {
            str(name) for name in metric_row.get("datasets") or [] if name
        }
        candidates: set[str] = set()
        for dataset in dataset_rows:
            dataset_name = str(dataset.get("name") or "")
            if dataset_name not in metric_datasets:
                continue
            primary = str(dataset.get("primary_time_dimension") or "").strip()
            if not primary:
                time_dimensions = [
                    str(name) for name in dataset.get("time_dimensions") or [] if name
                ]
                if len(time_dimensions) == 1:
                    primary = time_dimensions[0]
            if primary:
                candidate = primary if "." in primary else f"{dataset_name}.{primary}"
                if candidate in dimension_names:
                    candidates.add(candidate)
        return next(iter(candidates)) if len(candidates) == 1 else ""

    def _dry_run_dialect(
        self, binding: Any, connection: Optional[str]
    ) -> Optional[str]:
        """Dialect for compile-only calls: explicit config, else db_config type.

        With a connection profile the engine already knows the dialect; an
        agreeing explicit dialect is harmless, a conflicting one is a config
        error the engine reports. An explicitly configured dialect that the
        engine doesn't know is a config error (raise) — not silently dropped,
        which would emit DuckDB SQL for the wrong dialect. An unknown
        db_config *type* stays lenient (returns None → engine decides).
        """
        if self.config.dialect:
            resolved = resolve_engine_dialect(self.config.dialect, binding.DIALECTS)
            if resolved is None:
                raise SemanticCoreException(
                    f"unknown dialect {self.config.dialect!r}; "
                    f"supported: {', '.join(binding.DIALECTS)}"
                )
            return resolved
        db_config = self.config.db_config or {}
        return resolve_engine_dialect(db_config.get("type"), binding.DIALECTS)

    def _build_query(
        self,
        dimension_rows: List[Dict[str, Any]],
        metric_rows: List[Dict[str, Any]],
        *,
        metrics: List[str],
        dimensions: List[str],
        time_start: Optional[str],
        time_end: Optional[str],
        time_granularity: Optional[str],
        where: Optional[str],
        limit: Optional[int],
        order_by: Optional[List[str]],
    ) -> Dict[str, Any]:
        """Assemble the engine's MetricQuery dict. Pure: no engine I/O."""
        del metric_rows  # Kept for backward-compatible direct-call tests/callers.
        time_dimension_names = {
            row["name"] for row in dimension_rows if row.get("is_time")
        }

        def is_time_dimension(name: str) -> bool:
            return (
                name == "metric_time"
                or name in time_dimension_names
                or any(full.endswith(f".{name}") for full in time_dimension_names)
            )

        group_by: List[Dict[str, Any]] = []
        grain_attached = False
        requested_grain = str(time_granularity or "").strip().lower() or None
        for dimension in dimensions:
            item: Dict[str, Any] = {"field": dimension}
            if requested_grain and is_time_dimension(dimension):
                item["grain"] = requested_grain
                grain_attached = True
            group_by.append(item)

        if time_granularity and not grain_attached:
            model_time_dims = ["metric_time", *sorted(time_dimension_names)]
            raise SemanticValidationException(
                SemanticValidationError(
                    code="time_grain_required",
                    metrics=list(metrics),
                    required_dimensions=model_time_dims,
                    required_time_granularity=time_granularity,
                    suggested_retry=(
                        {
                            "metrics": list(metrics),
                            "dimensions": dimensions + model_time_dims[:1],
                            "time_granularity": time_granularity,
                        }
                        if model_time_dims
                        else None
                    ),
                    message=(
                        "time_granularity was given but no requested dimension is a "
                        "time dimension | time dimensions: "
                        f"{', '.join(model_time_dims)}"
                    ),
                )
            )

        query: Dict[str, Any] = {"metrics": list(metrics), "group_by": group_by}
        if where:
            query["where_sql"] = where
        if time_start or time_end:
            time_range: Dict[str, Any] = {"start": time_start, "end": time_end}
            if not any(is_time_dimension(item["field"]) for item in group_by):
                # The reserved time name lets the engine resolve each metric's
                # effective axis and report no_primary_time_dimension or
                # metric_time_conflict itself. The adapter must not guess from
                # raw dataset membership.
                time_range["dimension"] = "metric_time"
            query["time_range"] = time_range
        if order_by:
            query["order_by"] = [
                {"key": key[1:], "desc": True}
                if key.startswith("-")
                else {"key": key, "desc": False}
                for key in order_by
            ]
        if limit is not None:
            query["limit"] = int(limit)
        return query
