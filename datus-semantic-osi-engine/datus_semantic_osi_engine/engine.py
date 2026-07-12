# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Engine binding lifecycle: lazy import, connections wiring, mtime reload."""

from __future__ import annotations

import os
import tempfile
import threading
from typing import Any, Optional

import yaml

from datus_semantic_core.exceptions import SemanticCoreException

from datus_semantic_osi_engine.config import OSIEngineConfig
from datus_semantic_osi_engine.dialects import normalize_dialect

_INSTALL_HINT = (
    "datus-osi-engine is not installed; "
    "pip install 'datus-semantic-osi-engine[engine]'"
)


def load_binding() -> Any:
    """Import the datus-osi-engine bindings, failing with an install hint."""
    try:
        import datus_osi_engine
    except ImportError as exc:  # pragma: no cover - exercised via fake absence
        raise SemanticCoreException(_INSTALL_HINT) from exc
    return datus_osi_engine


class EngineHandle:
    """One engine per adapter instance, rebuilt when the model file changes.

    Every access re-stats ``semantic_model_path`` (one os.stat, negligible
    next to the call it guards) so edits to the OSI YAML are picked up
    without restarting the process.
    """

    def __init__(self, config: OSIEngineConfig):
        self._config = config
        self._lock = threading.Lock()
        self._engine: Optional[Any] = None
        self._model_mtime: Optional[float] = None
        self._connections_file: Optional[str] = None

    @property
    def profile_name(self) -> Optional[str]:
        """The connection profile to execute on, when one is configured."""
        config = self._config
        if config.connection:
            return config.connection
        if config.db_config:
            return config.datasource or "default"
        # A bare `datasource` name only means something with a connections
        # file to look it up in.
        if config.datasource and config.connections_path:
            return config.datasource
        return None

    def get(self) -> Any:
        config = self._config
        if not config.semantic_model_path:
            raise SemanticCoreException(
                "osi_engine adapter requires semantic_model_path (an OSI model file)"
            )
        try:
            mtime = os.path.getmtime(config.semantic_model_path)
        except OSError as exc:
            raise SemanticCoreException(
                f"cannot read semantic model {config.semantic_model_path!r}: {exc}"
            ) from exc
        with self._lock:
            if self._engine is None or mtime != self._model_mtime:
                self._engine = self._build(config)
                self._model_mtime = mtime
            return self._engine

    def _build(self, config: OSIEngineConfig) -> Any:
        binding = load_binding()
        try:
            return binding.Engine(
                model_path=config.semantic_model_path,
                connections_path=self._resolve_connections(config),
                pool_size=config.pool_size,
            )
        except binding.OsiError as exc:
            raise SemanticCoreException(
                f"osi-engine failed to load model {config.semantic_model_path!r}: {exc}"
            ) from exc

    def _resolve_connections(self, config: OSIEngineConfig) -> Optional[str]:
        if config.connections_path:
            return config.connections_path
        if not config.db_config:
            return None
        if self._connections_file is None:
            self._connections_file = self._write_connections_file(config)
        return self._connections_file

    def _write_connections_file(self, config: OSIEngineConfig) -> str:
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
            prefix="osi-connections-",
            delete=False,
        )
        with handle:
            yaml.safe_dump(payload, handle, default_flow_style=False)
        return handle.name
