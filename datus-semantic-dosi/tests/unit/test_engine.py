# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

import os

import pytest
from datus_semantic_core.exceptions import SemanticCoreException

from datus_semantic_dosi.config import DosiConfig
from datus_semantic_dosi.engine import (
    EngineHandle,
    EngineRegistry,
    datus_extension_version,
)


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


def test_registry_skips_model_removed_between_listing_and_stat(tmp_path, monkeypatch):
    stable = tmp_path / "stable.yaml"
    vanished = tmp_path / "vanished.yaml"
    stable.write_text("version: '0.2.0.dev0'\nsemantic_model: []\n")
    registry = EngineRegistry(DosiConfig(semantic_models_path=str(tmp_path)))
    monkeypatch.setattr(
        "datus_semantic_dosi.engine.resolve_model_files",
        lambda _config: [str(stable), str(vanished)],
    )

    signature, handles = registry.snapshot()

    assert [path for path, *_ in signature] == [str(stable)]
    assert [path for path, _ in handles] == [str(stable)]


def test_registry_rejects_snapshot_when_every_listed_model_disappears(
    tmp_path, monkeypatch
):
    vanished = tmp_path / "vanished.yaml"
    registry = EngineRegistry(DosiConfig(semantic_models_path=str(tmp_path)))
    monkeypatch.setattr(
        "datus_semantic_dosi.engine.resolve_model_files",
        lambda _config: [str(vanished)],
    )

    with pytest.raises(SemanticCoreException, match="no readable OSI model file"):
        registry.snapshot()


def test_resolve_model_files_finds_models_in_subdirectories(tmp_path):
    """Datus discovers authored models recursively -- its authoring inventory
    and its YAML-to-KB sync both walk the tree -- so a model it accepts under a
    subdirectory has to be visible here too, or the same file exists for
    authoring and for the knowledge base while being absent from queries."""
    from datus_semantic_dosi.engine import resolve_model_files

    document = "version: '0.2.0.dev0'\nsemantic_model: []\n"
    (tmp_path / "flat.yaml").write_text(document)
    nested = tmp_path / "orders"
    nested.mkdir()
    (nested / "orders.yml").write_text(document)

    resolved = resolve_model_files(DosiConfig(semantic_models_path=str(tmp_path)))

    assert [os.path.relpath(path, tmp_path) for path in resolved] == [
        "flat.yaml",
        os.path.join("orders", "orders.yml"),
    ]


def test_resolve_model_files_skips_the_metric_fragment_directory(tmp_path):
    """``metrics`` holds per-metric fragments, not whole documents. Datus omits
    it from both of its own model walks, and loading a fragment as a document
    fails."""
    from datus_semantic_dosi.engine import resolve_model_files

    (tmp_path / "flat.yaml").write_text("version: '0.2.0.dev0'\nsemantic_model: []\n")
    fragments = tmp_path / "metrics"
    fragments.mkdir()
    (fragments / "revenue.yml").write_text("name: revenue\n")

    resolved = resolve_model_files(DosiConfig(semantic_models_path=str(tmp_path)))

    assert [os.path.relpath(path, tmp_path) for path in resolved] == ["flat.yaml"]


def test_resolve_model_files_still_reports_an_empty_directory(tmp_path):
    from datus_semantic_dosi.engine import resolve_model_files

    (tmp_path / "notes").mkdir()

    with pytest.raises(SemanticCoreException, match="no OSI model file"):
        resolve_model_files(DosiConfig(semantic_models_path=str(tmp_path)))
