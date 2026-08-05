# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Registry integration for service_type dosi."""

from datus_semantic_core.registry import SemanticAdapterRegistry

import datus_semantic_dosi
from datus_semantic_dosi.adapter import DosiAdapter
from datus_semantic_dosi.config import DosiConfig


def test_register_binds_service_type():
    datus_semantic_dosi.register()
    metadata = SemanticAdapterRegistry.get_metadata("dosi")
    assert metadata is not None
    assert metadata.adapter_class is DosiAdapter
    assert metadata.config_class is DosiConfig
    assert metadata.display_name == "Dosi"


def test_create_adapter_roundtrip(model_file):
    datus_semantic_dosi.register()
    adapter = SemanticAdapterRegistry.create_adapter(
        "dosi", DosiConfig(semantic_model_path=str(model_file))
    )
    assert isinstance(adapter, DosiAdapter)
    assert adapter.service_type == "dosi"
