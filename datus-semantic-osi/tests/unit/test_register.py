# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""The OSI package registers an `osi` semantic adapter."""

from datus_semantic_core.registry import SemanticAdapterRegistry


def test_register_adds_osi_adapter():
    import datus_semantic_osi

    datus_semantic_osi.register()
    assert SemanticAdapterRegistry.is_registered("osi")
