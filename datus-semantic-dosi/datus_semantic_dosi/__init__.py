# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Datus semantic adapter backed by Dosi, the native Rust OSI engine.

A thin protocol translator: the OSI YAML is loaded, planned, compiled to
dialect SQL, and executed entirely inside the Rust engine (via the
``dosi-engine`` pyo3 bindings); this package only maps the Datus
semantic-adapter contract onto the engine's API and its structured errors
onto ``SemanticValidationError``.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("datus-semantic-dosi")
except PackageNotFoundError:  # Source checkout without an installed distribution.
    __version__ = "0.1.0"


def register() -> None:
    """Register the Dosi semantic adapter with the core registry.

    The dosi-engine binding is imported only when an adapter instance loads a
    model, keeping entry-point discovery lightweight.
    """
    from datus_semantic_core.registry import SemanticAdapterRegistry

    from datus_semantic_dosi.adapter import DosiAdapter
    from datus_semantic_dosi.config import DosiConfig

    SemanticAdapterRegistry.register(
        service_type="dosi",
        adapter_class=DosiAdapter,
        config_class=DosiConfig,
        display_name="Dosi",
    )
