# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Integration fixtures: real datus-osi-engine wheel + duckdb CLI required."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "orders"


def _real_binding_available() -> bool:
    try:
        import datus_osi_engine
    except ImportError:
        return False
    # The unit-test fake sets this marker; the real extension does not.
    return not getattr(datus_osi_engine, "__osi_fake__", False)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _real_binding_available(),
        reason="real datus-osi-engine bindings not installed",
    ),
    pytest.mark.skipif(shutil.which("duckdb") is None, reason="duckdb CLI not installed"),
]


@pytest.fixture(scope="session")
def model_path() -> str:
    return str(FIXTURES / "model.yaml")


@pytest.fixture(scope="session")
def seeded_db(tmp_path_factory) -> str:
    db = tmp_path_factory.mktemp("osi") / "orders.db"
    subprocess.run(
        ["duckdb", str(db)],
        input=(FIXTURES / "seed.sql").read_bytes(),
        check=True,
        capture_output=True,
    )
    return str(db)
