# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

import pytest
from datus_semantic_core.exceptions import SemanticCoreException
from datus_semantic_dosi.config import DosiConfig
from datus_semantic_dosi.engine import EngineHandle, datus_extension_version


def test_datus_extension_version_reads_active_engine_capabilities(
    fake_binding, monkeypatch
):
    monkeypatch.setattr(fake_binding, "DATUS_EXT", {"version": "7.3"})

    assert datus_extension_version() == "7.3"


def test_datus_extension_version_requires_capability_metadata(
    fake_binding, monkeypatch
):
    monkeypatch.delattr(fake_binding, "DATUS_EXT")

    with pytest.raises(SemanticCoreException, match=r"DATUS_EXT\.version"):
        datus_extension_version()


def test_non_object_model_root_is_mapped_by_native_loader(
    tmp_path, fake_binding, monkeypatch
):
    model_path = tmp_path / "model.yaml"
    model_path.write_text("- not\n- an\n- object\n")

    class RejectingEngine:
        def __init__(self, **kwargs):
            raise fake_binding.ModelError("invalid document root", code="invalid_model")

    monkeypatch.setattr(fake_binding, "Engine", RejectingEngine)

    with pytest.raises(SemanticCoreException, match="Dosi failed to load model"):
        EngineHandle(DosiConfig(semantic_model_path=str(model_path))).get()
