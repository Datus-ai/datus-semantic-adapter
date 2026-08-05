# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Unit-test fixtures.

The fake dosi-engine binding (in ``_fakes.py``) is installed into
``sys.modules["dosi_engine"]`` only for the duration of each test and the
previous entry is restored afterward — so it never shadows a real wheel in a
same-process integration run.
"""

from __future__ import annotations

import sys

import pytest

from _fakes import FakeEngine, build_fake_module

_FAKE_MODULE = build_fake_module()


@pytest.fixture(autouse=True)
def fake_binding(monkeypatch):
    """Install the fake binding for one test, restoring the prior module.

    ``monkeypatch.setitem`` records and restores whatever was in
    ``sys.modules["dosi_engine"]`` before (usually nothing), so the fake
    is scoped to unit tests only.
    """
    FakeEngine.instances.clear()
    _FAKE_MODULE.validate = build_fake_module().validate
    monkeypatch.setitem(sys.modules, "dosi_engine", _FAKE_MODULE)
    yield _FAKE_MODULE
    FakeEngine.instances.clear()


@pytest.fixture
def model_file(tmp_path):
    path = tmp_path / "model.yaml"
    path.write_text("version: '0.2.0.dev0'\nsemantic_model: []\n")
    return path


@pytest.fixture
def make_adapter(model_file):
    from datus_semantic_dosi.adapter import DosiAdapter
    from datus_semantic_dosi.config import DosiConfig

    def _make(**overrides):
        kwargs = {"semantic_model_path": str(model_file), **overrides}
        return DosiAdapter(DosiConfig(**kwargs))

    return _make
