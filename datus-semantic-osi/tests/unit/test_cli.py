# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""Tests for the `datus-osi compile` CLI."""

import json

import yaml

from datus_semantic_osi.cli import main

OSI_YAML = """
version: 0.2.0.dev0
semantic_model:
  - name: shop
    datasets:
      - name: orders
        source: orders
        primary_key: [order_id]
        fields:
          - name: order_date
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: order_date
            dimension: {is_time: true}
            custom_extensions:
              - vendor_name: DATUS
                data: '{"type":"time","time_granularity":"day"}'
    metrics:
      - name: order_count
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: "COUNT(DISTINCT order_id)"
        custom_extensions:
          - vendor_name: DATUS
            data: '{"dataset":"orders","time_dimension":"order_date"}'
"""


def test_cli_compile_writes_metricflow_yaml_and_ir(tmp_path):
    src = tmp_path / "model.yaml"
    src.write_text(OSI_YAML)
    out = tmp_path / "generated"
    ir_path = tmp_path / "ir.json"

    code = main(
        [
            "compile",
            "--input",
            str(src),
            "--output",
            str(out),
            "--backend",
            "metricflow",
            "--ir",
            str(ir_path),
        ]
    )
    assert code == 0
    assert (out / "semantic_models.yaml").exists()
    assert (out / "metrics.yaml").exists()

    ir = json.loads(ir_path.read_text())
    assert ir["metrics"][0]["name"] == "order_count"
    assert ir["metrics"][0]["kind"] == "aggregate"


def test_cli_compile_reports_business_error_for_bad_metric(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "version: 0.2.0.dev0\n"
        "semantic_model:\n"
        "  - name: shop\n"
        "    datasets:\n"
        "      - name: o\n"
        "        source: o\n"
        "    metrics:\n"
        "      - name: m\n"
        "        expression:\n"
        "          dialects:\n"
        "            - dialect: ANSI_SQL\n"
        "              expression: RANK() OVER (ORDER BY x)\n"
        "        custom_extensions:\n"
        "          - vendor_name: DATUS\n"
        "            data: '{\"dataset\":\"o\"}'\n"
    )
    out = tmp_path / "gen"
    code = main(["compile", "--input", str(bad), "--output", str(out)])
    assert code != 0


def test_cli_normalize_check_reports_actions_without_writing(tmp_path, capsys):
    (tmp_path / "alias.yml").write_text(
        """
version: 0.2.0.dev0
semantic_model:
  - name: shop
    datasets:
      - name: order_alias
        source: orders
        primary_key: [order_id]
    metrics:
      - name: order_count
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: "COUNT(DISTINCT order_id)"
        custom_extensions:
          - vendor_name: DATUS
            data: '{"dataset":"order_alias"}'
"""
    )
    (tmp_path / "orders.yml").write_text(
        """
version: 0.2.0.dev0
semantic_model:
  - name: shop
    datasets:
      - name: orders
        source: orders
        primary_key: [order_id]
"""
    )

    code = main(["normalize", "--input", str(tmp_path), "--check"])

    assert code == 0
    assert "Collapsed duplicate dataset `order_alias`" in capsys.readouterr().out
    alias_doc = yaml.safe_load((tmp_path / "alias.yml").read_text())
    metric = alias_doc["semantic_model"][0]["metrics"][0]
    assert "order_alias" in metric["custom_extensions"][0]["data"]


def test_cli_normalize_write_rewrites_directory_in_place(tmp_path):
    (tmp_path / "alias.yml").write_text(
        """
version: 0.2.0.dev0
semantic_model:
  - name: shop
    datasets:
      - name: order_alias
        source: orders
        primary_key: [order_id]
        fields:
          - name: channel
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: channel
            dimension: {is_time: false}
      - name: customers
        source: customers
        primary_key: [customer_id]
    relationships:
      - name: orders_to_customers
        from: order_alias
        to: customers
        from_columns: [customer_id]
        to_columns: [customer_id]
    metrics:
      - name: order_count
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: "COUNT(DISTINCT order_id)"
        custom_extensions:
          - vendor_name: DATUS
            data: '{"dataset":"order_alias"}'
"""
    )
    (tmp_path / "orders.yml").write_text(
        """
version: 0.2.0.dev0
semantic_model:
  - name: shop
    datasets:
      - name: orders
        source: orders
        primary_key: [order_id]
"""
    )

    code = main(["normalize", "--input", str(tmp_path), "--write"])

    assert code == 0
    normalized_doc = yaml.safe_load((tmp_path / "semantic_model.normalized.yml").read_text())
    model = normalized_doc["semantic_model"][0]
    assert {dataset["name"] for dataset in model["datasets"]} == {"orders", "customers"}
    assert '"dataset": "orders"' in model["metrics"][0]["custom_extensions"][0]["data"]
    assert model["relationships"][0]["from"] == "orders"
    assert "from_dataset" not in model["relationships"][0]
    orders_dataset = next(dataset for dataset in model["datasets"] if dataset["name"] == "orders")
    assert orders_dataset["fields"][0]["name"] == "channel"

    assert main(["normalize", "--input", str(tmp_path), "--check"]) == 0


def test_cli_normalize_write_single_file_dumps_core_relationship_shape(tmp_path):
    path = tmp_path / "model.yml"
    path.write_text(
        """
version: 0.2.0.dev0
semantic_model:
  - name: shop
    datasets:
      - name: order_alias
        source: orders
        primary_key: [order_id]
      - name: orders
        source: orders
        primary_key: [order_id]
      - name: customers
        source: customers
        primary_key: [customer_id]
    relationships:
      - name: orders_to_customers
        from: order_alias
        to: customers
        from_columns: [customer_id]
        to_columns: [customer_id]
    metrics:
      - name: order_count
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: "COUNT(DISTINCT order_id)"
        custom_extensions:
          - vendor_name: DATUS
            data: '{"dataset":"order_alias"}'
"""
    )

    code = main(["normalize", "--input", str(path), "--write"])

    assert code == 0
    doc = yaml.safe_load(path.read_text())
    assert doc["version"] == "0.2.0.dev0"
    model = doc["semantic_model"][0]
    assert model["name"] == "shop"
    assert [dataset["name"] for dataset in model["datasets"]] == ["orders", "customers"]
    assert '"dataset": "orders"' in model["metrics"][0]["custom_extensions"][0]["data"]
    assert model["relationships"] == [
        {
            "name": "orders_to_customers",
            "from": "orders",
            "to": "customers",
            "from_columns": ["customer_id"],
            "to_columns": ["customer_id"],
        }
    ]
