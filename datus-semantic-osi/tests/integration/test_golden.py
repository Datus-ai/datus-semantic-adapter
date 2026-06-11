# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""Golden tests: OSI input -> expected IR + expected MetricFlow YAML.

Each case under ``tests/golden/<case>/`` has ``input_osi.yaml`` and the expected
``expected_ir.json`` + ``expected_metricflow/{semantic_models,metrics}.yaml``.

Regenerate expectations with ``DATUS_OSI_UPDATE_GOLDEN=1 pytest ...`` after an
intentional change. Each golden artifact is also checked against real MetricFlow.
"""

import json
import os
from pathlib import Path

import pytest

from datus_semantic_osi.compiler import compile_document
from datus_semantic_osi.metricflow_backend import lower_to_metricflow
from datus_semantic_osi.profile import load_osi_path

GOLDEN_DIR = Path(__file__).resolve().parents[1] / "golden"
UPDATE = os.getenv("DATUS_OSI_UPDATE_GOLDEN") == "1"
CASES = sorted(p.name for p in GOLDEN_DIR.iterdir() if (p / "input_osi.yaml").exists())


def _ir_json(model) -> str:
    return json.dumps(model.model_dump(mode="json"), indent=2, sort_keys=True) + "\n"


@pytest.mark.parametrize("case", CASES)
def test_golden_case(case, tmp_path):
    case_dir = GOLDEN_DIR / case
    model = compile_document(load_osi_path(str(case_dir / "input_osi.yaml")))
    art = lower_to_metricflow(model)

    ir_text = _ir_json(model)
    sm_text = art.semantic_models_yaml()
    metrics_text = art.metrics_yaml()

    ir_path = case_dir / "expected_ir.json"
    mf_dir = case_dir / "expected_metricflow"
    sm_path = mf_dir / "semantic_models.yaml"
    metrics_path = mf_dir / "metrics.yaml"

    if UPDATE:
        mf_dir.mkdir(parents=True, exist_ok=True)
        ir_path.write_text(ir_text)
        sm_path.write_text(sm_text)
        metrics_path.write_text(metrics_text)

    assert ir_path.exists(), (
        f"missing golden IR for {case}; run with DATUS_OSI_UPDATE_GOLDEN=1"
    )
    assert ir_text == ir_path.read_text(), f"IR drift for {case}"
    assert sm_text == sm_path.read_text(), f"semantic_models.yaml drift for {case}"
    assert metrics_text == metrics_path.read_text(), f"metrics.yaml drift for {case}"

    # the golden MetricFlow artifact must pass real MetricFlow validation
    pytest.importorskip("metricflow")
    from metricflow.model.model_validator import ModelValidator
    from metricflow.model.parsing.dir_to_model import (
        parse_directory_of_yaml_files_to_model,
    )

    art.write(tmp_path)
    build = parse_directory_of_yaml_files_to_model(str(tmp_path))
    assert [str(e) for e in build.issues.errors] == [], f"{case} parse errors"
    semantic = ModelValidator().validate_model(build.model)
    assert [str(e) for e in semantic.issues.errors] == [], f"{case} semantic errors"
