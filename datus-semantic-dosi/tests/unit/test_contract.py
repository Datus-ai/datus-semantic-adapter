# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Core semantic-adapter contract, run against the fake engine binding."""

import pytest
from datus_semantic_core.testing import make_semantic_contract_suite

from datus_semantic_dosi.adapter import DosiAdapter
from datus_semantic_dosi.config import DosiConfig


@pytest.fixture(autouse=True)
def _model_path(model_file, monkeypatch):
    monkeypatch.setenv("DOSI_CONTRACT_MODEL", str(model_file))


def _factory():
    import os

    return DosiAdapter(
        DosiConfig(semantic_model_path=os.environ["DOSI_CONTRACT_MODEL"])
    )


TestDosiContract = make_semantic_contract_suite(
    _factory,
    sample_metric_name="order_count",
    sample_dimension_name="orders.status",
)
