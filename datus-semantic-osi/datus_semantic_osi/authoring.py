# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""File-level authoring for OSI metrics.

The OSI ``subject/semantic_models`` YAML files are the source of truth. These
helpers read/mutate a single metric *in place* inside its owning
``semantic_model.metrics`` list, preserving the rest of the document (datasets,
relationships, sibling metrics). They operate on the raw core-schema dicts — not
the compiled IR — so round-trips keep OSI-native fields (``expression.dialects``,
``ai_context``, ``custom_extensions``) intact.

This module is a backend/editor surface and is not wired into the agent tool
registry; see ``datus_semantic_core.authoring``.
"""

from __future__ import annotations

import glob
import json
import os
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import yaml

from datus_semantic_core.authoring import MetricMutationResult, MetricSource
from datus_semantic_core.models import ValidationIssue, ValidationResult

from .errors import OSIValidationError
from .profile import (
    CORE_SCHEMA_VERSION,
    DATUS_VENDOR,
    parse_osi,
    validate_osi_core_schema,
)

FORMAT = "osi"


@dataclass
class _MetricLocation:
    file_path: str
    docs: List[Any]  # every YAML document in the file
    doc_index: int
    model_index: int  # index within docs[doc_index]["semantic_model"]
    metric_index: int  # index within model["metrics"]

    @property
    def model(self) -> Dict[str, Any]:
        return self.docs[self.doc_index]["semantic_model"][self.model_index]

    @property
    def node(self) -> Dict[str, Any]:
        return self.model["metrics"][self.metric_index]


def _yaml_files(root: str) -> List[str]:
    return sorted(
        glob.glob(os.path.join(root, "**", "*.yaml"), recursive=True)
        + glob.glob(os.path.join(root, "**", "*.yml"), recursive=True)
    )


def _read_docs(file_path: str) -> List[Any]:
    with open(file_path, encoding="utf-8") as fh:
        return list(yaml.safe_load_all(fh.read())) or []


def _iter_core_models(docs: List[Any]):
    """Yield (doc_index, model_index, model_dict) for every core semantic model."""
    for doc_index, doc in enumerate(docs):
        if not isinstance(doc, dict):
            continue
        models = doc.get("semantic_model")
        if not isinstance(models, list):
            continue
        for model_index, model in enumerate(models):
            if isinstance(model, dict):
                yield doc_index, model_index, model


def _dump_yaml(obj: Any) -> str:
    return yaml.safe_dump(
        obj, sort_keys=False, allow_unicode=True, default_flow_style=False
    )


def _datus_hints(node: Dict[str, Any]) -> Dict[str, Any]:
    """Merge every DATUS ``custom_extensions`` payload on ``node`` into one dict."""
    merged: Dict[str, Any] = {}
    for ext in node.get("custom_extensions") or []:
        if (
            not isinstance(ext, dict)
            or str(ext.get("vendor_name", "")).upper() != DATUS_VENDOR
        ):
            continue
        raw = ext.get("data")
        if isinstance(raw, dict):
            merged.update(raw)
        elif isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                merged.update(parsed)
    return merged


def _set_datus_hints(node: Dict[str, Any], updates: Dict[str, Any]) -> None:
    """Write ``updates`` into node's DATUS custom extension, preserving JSON-string style."""
    extensions = node.setdefault("custom_extensions", [])
    for ext in extensions:
        if (
            isinstance(ext, dict)
            and str(ext.get("vendor_name", "")).upper() == DATUS_VENDOR
        ):
            raw = ext.get("data")
            if isinstance(raw, str):
                try:
                    data = json.loads(raw) if raw.strip() else {}
                except json.JSONDecodeError:
                    data = {}
                data.update(updates)
                ext["data"] = json.dumps(
                    data, ensure_ascii=False, separators=(",", ":")
                )
            else:
                data = dict(raw) if isinstance(raw, dict) else {}
                data.update(updates)
                ext["data"] = data
            return
    # No DATUS entry yet — add one using the JSON-string convention.
    extensions.append(
        {
            "vendor_name": DATUS_VENDOR,
            "data": json.dumps(updates, ensure_ascii=False, separators=(",", ":")),
        }
    )


class OSIMetricAuthor:
    """Read/write/delete/validate a single OSI metric in its source file."""

    def __init__(self, semantic_models_path: str):
        self._root = semantic_models_path

    # ---- location -------------------------------------------------------

    def _require_root(self) -> str:
        if not self._root or not os.path.isdir(self._root):
            raise OSIValidationError(
                f"semantic_models_path is not a directory: {self._root}"
            )
        return self._root

    def _locate(self, metric_name: str) -> Optional[_MetricLocation]:
        for file_path in _yaml_files(self._require_root()):
            try:
                docs = _read_docs(file_path)
            except yaml.YAMLError:
                continue
            for doc_index, model_index, model in _iter_core_models(docs):
                for metric_index, metric in enumerate(model.get("metrics") or []):
                    if isinstance(metric, dict) and metric.get("name") == metric_name:
                        return _MetricLocation(
                            file_path, docs, doc_index, model_index, metric_index
                        )
        return None

    def _model_owning_dataset(
        self, dataset: Optional[str]
    ) -> Optional[Tuple[str, List[Any], int, int]]:
        """Find (file_path, docs, doc_index, model_index) whose model declares ``dataset``."""
        if not dataset:
            return None
        for file_path in _yaml_files(self._require_root()):
            try:
                docs = _read_docs(file_path)
            except yaml.YAMLError:
                continue
            for doc_index, model_index, model in _iter_core_models(docs):
                for ds in model.get("datasets") or []:
                    if isinstance(ds, dict) and ds.get("name") == dataset:
                        return file_path, docs, doc_index, model_index
        return None

    def _sole_model(self) -> Optional[Tuple[str, List[Any], int, int]]:
        found: List[Tuple[str, List[Any], int, int]] = []
        for file_path in _yaml_files(self._require_root()):
            try:
                docs = _read_docs(file_path)
            except yaml.YAMLError:
                continue
            for doc_index, model_index, _model in _iter_core_models(docs):
                found.append((file_path, docs, doc_index, model_index))
                if len(found) > 1:
                    return None
        return found[0] if len(found) == 1 else None

    # ---- read -----------------------------------------------------------

    def read(self, metric_name: str) -> MetricSource:
        loc = self._locate(metric_name)
        if loc is None:
            raise OSIValidationError(
                f"Metric `{metric_name}` was not found in {self._root}."
            )
        return MetricSource(
            name=metric_name,
            format=FORMAT,
            text=_dump_yaml(loc.node),
            semantic_model=str(loc.model.get("name") or "") or None,
            file_path=loc.file_path,
        )

    # ---- write ----------------------------------------------------------

    @staticmethod
    def _parse_node(source: str) -> Dict[str, Any]:
        try:
            node = yaml.safe_load(source)
        except yaml.YAMLError as exc:
            raise OSIValidationError(f"Invalid YAML: {exc}") from exc
        # Tolerate a MetricFlow-style {metric: {...}} wrapper for convenience.
        if (
            isinstance(node, dict)
            and set(node.keys()) == {"metric"}
            and isinstance(node["metric"], dict)
        ):
            node = node["metric"]
        if not isinstance(node, dict) or not node.get("name"):
            raise OSIValidationError(
                "Metric source must be a mapping with a `name` field."
            )
        return node

    def write(
        self,
        metric_name: str,
        source: str,
        *,
        subject_path: Optional[List[str]] = None,
        create: bool = False,
    ) -> MetricMutationResult:
        node = self._parse_node(source)
        if node.get("name") != metric_name:
            raise OSIValidationError(
                f"Metric name in source (`{node.get('name')}`) does not match `{metric_name}`."
            )
        if subject_path is not None:
            _set_datus_hints(node, {"subject_path": list(subject_path)})

        existing = self._locate(metric_name)
        if create and existing is not None:
            raise OSIValidationError(
                f"Metric `{metric_name}` already exists at {existing.file_path}."
            )
        if not create and existing is None:
            raise OSIValidationError(
                f"Metric `{metric_name}` does not exist; use create=True."
            )

        if existing is not None:
            existing.model["metrics"][existing.metric_index] = node
            file_path, docs, doc_index = (
                existing.file_path,
                existing.docs,
                existing.doc_index,
            )
            model_index = existing.model_index
        else:
            target = (
                self._model_owning_dataset(_datus_hints(node).get("dataset"))
                or self._sole_model()
            )
            if target is None:
                raise OSIValidationError(
                    "Cannot resolve a target semantic model for the new metric. "
                    "Set the metric's DATUS `dataset` to a dataset declared by an existing "
                    "semantic model, or ensure exactly one semantic model exists."
                )
            file_path, docs, doc_index, model_index = target
            docs[doc_index]["semantic_model"][model_index].setdefault(
                "metrics", []
            ).append(node)

        self._validate_doc(docs[doc_index])
        _atomic_write(file_path, docs)
        return MetricMutationResult(
            name=metric_name,
            format=FORMAT,
            file_path=file_path,
            semantic_model=str(
                docs[doc_index]["semantic_model"][model_index].get("name") or ""
            )
            or None,
            created=create,
            affected_paths=[file_path],
        )

    # ---- delete ---------------------------------------------------------

    def delete(self, metric_name: str) -> MetricMutationResult:
        loc = self._locate(metric_name)
        if loc is None:
            raise OSIValidationError(
                f"Metric `{metric_name}` was not found in {self._root}."
            )
        model_name = str(loc.model.get("name") or "") or None
        del loc.model["metrics"][loc.metric_index]
        _atomic_write(loc.file_path, loc.docs)
        return MetricMutationResult(
            name=metric_name,
            format=FORMAT,
            file_path=loc.file_path,
            semantic_model=model_name,
            deleted=True,
            affected_paths=[loc.file_path],
        )

    # ---- validate -------------------------------------------------------

    @staticmethod
    def _validate_doc(doc: Dict[str, Any]) -> None:
        """Schema + semantic validation of a full core document; raises on error."""
        validate_osi_core_schema(doc)
        parse_osi(doc)  # runs merge + profile parsing, surfacing structural errors

    def validate(
        self, source: str, *, metric_name: Optional[str] = None
    ) -> ValidationResult:
        try:
            node = self._parse_node(source)
        except OSIValidationError as exc:
            return ValidationResult(
                valid=False,
                issues=[ValidationIssue(severity="error", message=str(exc))],
            )

        name = metric_name or str(node.get("name"))
        # Validate the metric inside its real model context when we can resolve one,
        # otherwise wrap it in a minimal single-model document for schema checks.
        existing = self._locate(name) if name else None
        if existing is not None:
            model = json_clone(existing.model)
            replaced = False
            for idx, metric in enumerate(model.get("metrics") or []):
                if isinstance(metric, dict) and metric.get("name") == name:
                    model["metrics"][idx] = node
                    replaced = True
                    break
            if not replaced:
                model.setdefault("metrics", []).append(node)
        else:
            target = (
                self._model_owning_dataset(_datus_hints(node).get("dataset"))
                or self._sole_model()
            )
            if target is not None:
                file_path, docs, doc_index, model_index = target
                model = json_clone(docs[doc_index]["semantic_model"][model_index])
                model.setdefault("metrics", []).append(node)
            else:
                model = {"name": "authoring_preview", "datasets": [], "metrics": [node]}

        doc = {"version": CORE_SCHEMA_VERSION, "semantic_model": [model]}
        try:
            self._validate_doc(doc)
        except Exception as exc:  # noqa: BLE001 - surface any validation failure to the caller
            return ValidationResult(
                valid=False,
                issues=[ValidationIssue(severity="error", message=str(exc))],
            )
        return ValidationResult(valid=True, issues=[])


def json_clone(value: Any) -> Any:
    """Deep-copy plain YAML/JSON data without importing copy for every call site."""
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def _atomic_write(file_path: str, docs: List[Any]) -> None:
    directory = os.path.dirname(file_path) or "."
    payload = yaml.safe_dump_all(
        docs, sort_keys=False, allow_unicode=True, default_flow_style=False
    )
    fd, tmp_path = tempfile.mkstemp(dir=directory, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
        os.replace(tmp_path, file_path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
