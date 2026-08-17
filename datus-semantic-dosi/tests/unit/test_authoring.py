# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Dosi uses the shared OSI file editor with native validation.

Authoring only touches the YAML files (never the Rust binding), so these run
against the fake binding like the rest of the unit suite.
"""

from __future__ import annotations

import textwrap

import pytest
import yaml

MODEL = textwrap.dedent(
    """\
    version: 0.2.0.dev0
    semantic_model:
      - name: jeff_shop_live
        datasets:
          - name: raw_orders
            source: jeff_shop.raw_orders
            primary_key: [id]
            fields:
              - name: order_total
                expression:
                  dialects:
                    - dialect: STARROCKS
                      expression: order_total
        metrics:
          - name: daily_order_count
            description: "Daily order count."
            expression:
              dialects:
                - dialect: STARROCKS
                  expression: "COUNT(DISTINCT id)"
            custom_extensions:
              - vendor_name: DATUS
                data: '{"dataset":"raw_orders","subject_path":["operations","daily"]}'
    """
)


@pytest.fixture
def osi_adapter(tmp_path, make_adapter):
    model_path = tmp_path / "jeff_shop_live.yml"
    model_path.write_text(MODEL)
    return make_adapter(semantic_model_path=str(model_path)), model_path


def test_read_returns_osi_native_yaml(osi_adapter):
    adapter, _ = osi_adapter
    src = adapter.read_metric_source("daily_order_count")
    assert src.format == "osi"
    assert src.semantic_model == "jeff_shop_live"
    node = yaml.safe_load(src.text)
    assert node["expression"]["dialects"][0]["dialect"] == "STARROCKS"
    assert "type" not in node and "locked_metadata" not in node


def test_edit_preserves_structure(osi_adapter):
    adapter, model_path = osi_adapter
    src = adapter.read_metric_source("daily_order_count")
    edited = src.text.replace("Daily order count.", "Edited.")
    res = adapter.write_metric_source("daily_order_count", edited, subject_path=["ops"])
    assert res.created is False
    on_disk = yaml.safe_load(model_path.read_text())
    model = on_disk["semantic_model"][0]
    assert model["metrics"][0]["description"] == "Edited."
    assert model["datasets"][0]["name"] == "raw_orders"


def test_validate_and_delete(osi_adapter):
    adapter, model_path = osi_adapter
    src = adapter.read_metric_source("daily_order_count")
    assert adapter.validate_metric_source(
        src.text, metric_name="daily_order_count"
    ).valid

    res = adapter.delete_metric_source("daily_order_count")
    assert res.deleted is True
    assert yaml.safe_load(model_path.read_text())["semantic_model"][0]["metrics"] == []


def test_native_structured_window_round_trips_through_authoring(osi_adapter):
    adapter, model_path = osi_adapter
    source = adapter.read_metric_source("daily_order_count").text
    node = yaml.safe_load(source)
    node["custom_extensions"][0]["data"] = (
        '{"v":"1.2","dataset":"raw_orders","window":'
        '{"type":"rolling","function":"sum","periods":7}}'
    )
    updated = yaml.safe_dump(node, sort_keys=False)

    assert adapter.validate_metric_source(
        updated, metric_name="daily_order_count"
    ).valid
    adapter.write_metric_source("daily_order_count", updated)

    on_disk = yaml.safe_load(model_path.read_text())
    payload = yaml.safe_load(
        on_disk["semantic_model"][0]["metrics"][0]["custom_extensions"][0]["data"]
    )
    assert payload["window"]["type"] == "rolling"


def test_legacy_window_is_rejected(osi_adapter):
    adapter, _ = osi_adapter
    source = adapter.read_metric_source("daily_order_count").text
    node = yaml.safe_load(source)
    node["custom_extensions"][0]["data"] = (
        '{"dataset":"raw_orders","window":"7 days","window_aggregation":"sum"}'
    )

    result = adapter.validate_metric_source(
        yaml.safe_dump(node, sort_keys=False), metric_name="daily_order_count"
    )
    assert result.valid is False


async def test_metadata_reads_the_same_first_datus_entry_as_native_engine(
    make_adapter, model_file
):
    model_file.write_text(
        """
version: 0.2.0.dev0
semantic_model:
  - name: orders_model
    datasets: []
    metrics:
      - name: running_revenue
        custom_extensions:
          - vendor_name: " DATUS "
            data: '{"v":"1.3","unit":"USD"}'
          - vendor_name: datus
            data: >-
              {"v":"1.3","unit":"EUR",
              "window":{"type":"cumulative","function":"sum"}}
""".lstrip()
    )

    metric = next(
        metric
        for metric in await make_adapter().list_metrics()
        if metric.name == "running_revenue"
    )

    assert metric.type == "window"
    assert metric.unit == "EUR"


def test_authoring_root_falls_back_to_models_dir(tmp_path, make_adapter):
    (tmp_path / "jeff_shop_live.yml").write_text(MODEL)
    # Config with only semantic_models_path (a directory), no explicit file.
    adapter = make_adapter(semantic_model_path=None, semantic_models_path=str(tmp_path))
    assert adapter.read_metric_source("daily_order_count").name == "daily_order_count"


@pytest.mark.parametrize("version", ["1.3", "1.4"])
def test_datus_extension_authoring_spec_matches_native_version(
    monkeypatch, fake_binding, version
):
    from datus_semantic_dosi.authoring_spec import datus_extension_authoring_spec_text
    from datus_semantic_dosi.engine import datus_extension_version

    monkeypatch.setattr(fake_binding, "DATUS_EXT", {"version": version})
    active_version = datus_extension_version()
    spec = datus_extension_authoring_spec_text("STARROCKS")

    assert active_version == version
    assert f'extension_version: "{active_version}"' in spec
    assert "dialect: STARROCKS" in spec
    assert "structured_window:" in spec


def test_datus_extension_1_4_authoring_contract(monkeypatch, fake_binding):
    from datus_semantic_dosi.authoring_spec import datus_extension_authoring_spec_text

    monkeypatch.setattr(fake_binding, "DATUS_EXT", {"version": "1.4"})
    spec = yaml.safe_load(datus_extension_authoring_spec_text("GAUSSDB"))

    assert spec["spec"]["extension_version"] == "1.4"
    assert spec["envelope"]["keys"]["v"]["generated_value"] == "1.4"
    assert set(spec["structured_derive"]["families"]) == {"filter", "compose"}
    assert spec["explicit_measure"]["required"] == ["name"]
    assert (
        '"requires":["measure"]'
        in spec["examples"]["explicitly_named_measure"]["custom_extensions"][0]["data"]
    )
    assert (
        spec["examples"]["dataset_and_time_field"]["fields"][0]["expression"][
            "dialects"
        ][0]["dialect"]
        == "GAUSSDB"
    )


def test_authoring_spec_reports_unrecognized_core_spec_layout(monkeypatch):
    import re

    from datus_semantic_core.exceptions import SemanticCoreException
    from datus_semantic_dosi import authoring_spec

    monkeypatch.setattr(authoring_spec, "_DIALECTS_BLOCK_RE", re.compile("absent"))

    with pytest.raises(SemanticCoreException, match="dialects block"):
        authoring_spec.authoring_spec_text("STARROCKS")


def test_missing_extension_spec_uses_semantic_error(monkeypatch, fake_binding):
    from datus_semantic_core.exceptions import SemanticCoreException
    from datus_semantic_dosi.authoring_spec import datus_extension_authoring_spec_text

    monkeypatch.setattr(fake_binding, "DATUS_EXT", {"version": "9.9"})

    with pytest.raises(SemanticCoreException, match=r"engine version '9\.9'"):
        datus_extension_authoring_spec_text("STARROCKS")


def test_validation_payload_maps_yaml_serialization_error():
    from datus_semantic_dosi.authoring import (
        DosiAuthoringError,
        dosi_validation_payload,
    )

    with pytest.raises(DosiAuthoringError, match="cannot serialize OSI document"):
        dosi_validation_payload({"semantic_model": object()})
