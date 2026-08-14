# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Engine binding lifecycle: lazy import, connections wiring, file reload."""

from __future__ import annotations

import glob
import os
import tempfile
import threading
import weakref
from pathlib import Path
from typing import Any, Optional

import yaml
from datus_semantic_core.exceptions import SemanticCoreException

from datus_semantic_dosi.config import DosiConfig
from datus_semantic_dosi.dialects import normalize_dialect
from datus_semantic_dosi.model import (
    legacy_window_error,
    legacy_window_findings,
    load_document,
)

_INSTALL_HINT = (
    "dosi-engine is missing from the datus-semantic-dosi installation; "
    "for local development install the native checkout with "
    "`uv pip install -e <osi-engine>/crates/dosi-py`"
)


def _unlink_quietly(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


def resolve_model_files(config: DosiConfig) -> list[str]:
    """Resolve every model file available to an adapter configuration.

    ``semantic_model_path`` remains an explicit single-file pin. Directory
    configurations return every top-level OSI model in deterministic order so
    the adapter can build a datasource-wide metric catalog over one native
    engine per file.
    """
    if config.semantic_model_path:
        return [config.semantic_model_path]
    models_dir = config.semantic_models_path
    if models_dir:
        candidates = sorted(
            path
            for ext in ("*.yaml", "*.yml", "*.json")
            for path in glob.glob(os.path.join(models_dir, ext))
        )
        if candidates:
            return candidates
        raise SemanticCoreException(
            f"no OSI model file (*.yaml/*.yml/*.json) in {models_dir!r}"
        )
    raise SemanticCoreException(
        "dosi adapter requires semantic_model_path (an OSI model file) "
        "or semantic_models_path (a directory containing one)"
    )


def resolve_model_file(config: DosiConfig) -> str:
    """The OSI model file to load: explicit semantic_model_path, else the sole
    model file in semantic_models_path (the Datus directory convention).

    Raises SemanticCoreException when nothing resolves, or when a directory
    holds several models (the engine loads exactly one).
    """
    candidates = resolve_model_files(config)
    if len(candidates) == 1:
        return candidates[0]
    models_dir = config.semantic_models_path
    if models_dir:
        raise SemanticCoreException(
            f"{len(candidates)} model files in {models_dir!r}; "
            "set semantic_model_path to select one"
        )
    raise AssertionError("multiple models cannot resolve from semantic_model_path")


def load_binding() -> Any:
    """Import the mandatory dosi-engine binding with a repair hint."""
    try:
        import dosi_engine
    except ImportError as exc:  # pragma: no cover - exercised via fake absence
        raise SemanticCoreException(_INSTALL_HINT) from exc
    return dosi_engine


def datus_extension_version() -> str:
    """Return the DATUS extension version implemented by the active engine."""
    binding = load_binding()
    capabilities = getattr(binding, "DATUS_EXT", None)
    version = capabilities.get("version") if isinstance(capabilities, dict) else None
    if version is None or not str(version).strip():
        raise SemanticCoreException(
            "the installed dosi-engine does not expose DATUS_EXT.version; "
            "install a current dosi-engine build before authoring versioned "
            "DATUS extensions"
        )
    return str(version).strip()


class EngineHandle:
    """One engine per adapter instance, rebuilt when the model file changes.

    Every access re-stats ``semantic_model_path`` (one os.stat, negligible
    next to the call it guards) so edits to the OSI YAML are picked up
    without restarting the process.
    """

    def __init__(self, config: DosiConfig):
        self._config = config
        self._lock = threading.Lock()
        self._engine: Optional[Any] = None
        self._model_signature: Optional[tuple[int, int]] = None
        self._connections_file: Optional[str] = None

    @property
    def profile_name(self) -> Optional[str]:
        """The connection profile to execute on, when one is configured.

        Precedence mirrors _resolve_connections: an explicit connections_path
        is authoritative over db_config, so it is checked first — otherwise
        the name here (from db_config) would not exist in the file actually
        used.
        """
        config = self._config
        if config.connection:
            return config.connection
        if config.connections_path:
            # A bare `datasource` name is looked up in the connections file;
            # None lets the engine pick the file's default profile.
            return config.datasource
        if config.db_config:
            return config.datasource or "default"
        return None

    def model_file(self) -> str:
        """The resolved OSI model file path (raises if unresolvable)."""
        return resolve_model_file(self._config)

    def get(self) -> Any:
        config = self._config
        model_file = resolve_model_file(config)
        try:
            stat = os.stat(model_file)
        except OSError as exc:
            raise SemanticCoreException(
                f"cannot read semantic model {model_file!r}: {exc}"
            ) from exc
        with self._lock:
            signature = (stat.st_mtime_ns, stat.st_size)
            if self._engine is None or signature != self._model_signature:
                self._engine = self._build(config, model_file)
                self._model_signature = signature
            return self._engine

    def _build(self, config: DosiConfig, model_file: str) -> Any:
        binding = load_binding()
        try:
            findings = legacy_window_findings(load_document(model_file))
        except (OSError, TypeError, ValueError, yaml.YAMLError):
            # The native loader owns syntax/schema diagnostics. This preflight
            # exists only to catch syntactically valid legacy hints that Dosi
            # would otherwise treat as inert metadata.
            findings = []
        if findings:
            raise SemanticCoreException(legacy_window_error(findings))
        try:
            return binding.Engine(
                model_path=model_file,
                connections_path=self._resolve_connections(config),
                pool_size=config.pool_size,
            )
        except binding.OsiError as exc:
            raise SemanticCoreException(
                f"Dosi failed to load model {model_file!r}: {exc}"
            ) from exc

    def _resolve_connections(self, config: DosiConfig) -> Optional[str]:
        if config.connections_path:
            return config.connections_path
        if not config.db_config:
            return None
        if self._connections_file is None:
            self._connections_file = self._write_connections_file(config)
        return self._connections_file

    def _write_connections_file(self, config: DosiConfig) -> str:
        """Materialize `db_config` as a `datasources:` YAML the engine reads.

        The engine's connections vocabulary IS the agent.yml datasource
        vocabulary, so fields pass through verbatim — only the `type` alias
        is normalized (the engine derives the dialect from it) and the entry
        is marked default so connection-less execution lands on it.
        """
        entry = dict(config.db_config or {})
        dialect = normalize_dialect(entry.get("type"))
        if dialect:
            entry["type"] = dialect
        entry.setdefault("default", True)
        payload = {"datasources": {self.profile_name or "default": entry}}
        handle = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".yaml",
            prefix="dosi-connections-",
            delete=False,
        )
        with handle:
            yaml.safe_dump(payload, handle, default_flow_style=False)
        # Tie the temp file's lifetime to this handle so it doesn't leak for
        # the process lifetime (relevant when adapters are created per-request).
        weakref.finalize(self, _unlink_quietly, handle.name)
        return handle.name


class EngineRegistry:
    """Directory-aware lifecycle for one native Dosi engine per model file."""

    def __init__(self, config: DosiConfig):
        self._config = config
        self._lock = threading.Lock()
        self._signature: tuple[tuple[str, int, int], ...] = ()
        self._handles: dict[str, EngineHandle] = {}

    @staticmethod
    def _file_signature(path: str) -> tuple[str, int, int]:
        try:
            stat = Path(path).stat()
        except OSError as exc:
            raise SemanticCoreException(
                f"cannot read semantic model {path!r}: {exc}"
            ) from exc
        return path, stat.st_mtime_ns, stat.st_size

    def snapshot(
        self,
    ) -> tuple[tuple[tuple[str, int, int], ...], tuple[tuple[str, EngineHandle], ...]]:
        """Return a stable file signature and matching engine handles."""
        with self._lock:
            files = resolve_model_files(self._config)
            signature = tuple(self._file_signature(path) for path in files)
            if signature != self._signature:
                previous = self._handles
                self._handles = {
                    path: previous.get(path)
                    or EngineHandle(
                        self._config.model_copy(update={"semantic_model_path": path})
                    )
                    for path in files
                }
                self._signature = signature
            return self._signature, tuple((path, self._handles[path]) for path in files)
