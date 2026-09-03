# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Configuration for the Dosi semantic adapter."""

from __future__ import annotations

from typing import Any, Dict, Optional

from datus_semantic_core.config import SemanticAdapterConfig


class DosiConfig(SemanticAdapterConfig):
    """Adapter configuration.

    The adapter passes ``db_config`` to the Python engine as an in-memory
    datasource mapping. Connections-file discovery belongs to the Rust CLI and
    server surfaces, not this embedded adapter path.
    """

    service_type: str = "dosi"
    # Path to the OSI semantic model file (.yaml/.yml/.json). Takes precedence
    # over semantic_models_path.
    semantic_model_path: Optional[str] = None
    # Directory of OSI models (Datus convention, e.g. subject/semantic_models/
    # <datasource>). Used when semantic_model_path is unset: discovery and
    # queries route across every top-level YAML/YML/JSON model in the directory.
    semantic_models_path: Optional[str] = None
    # Named connection profile; falls back to the base-class `datasource`.
    connection: Optional[str] = None
    # Inline datasource entry (agent.yml vocabulary: type/host/port/...).
    db_config: Optional[Dict[str, Any]] = None
    # Explicit SQL dialect for dry-run compilation without a connection.
    dialect: Optional[str] = None
    # Per-profile connection-pool cap inside the engine.
    pool_size: int = 8
