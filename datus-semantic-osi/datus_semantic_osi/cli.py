# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""``datus-osi`` command line interface.

Compiles OSI authoring into a backend artifact (default: MetricFlow YAML):

    datus-osi compile --input model.yaml --output /tmp/mf --backend metricflow
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional

import yaml

from datus_semantic_osi.backend import make_backend
from datus_semantic_osi.compiler import compile_document
from datus_semantic_osi.errors import OSIError
from datus_semantic_osi.normalizer import NormalizationResult, normalize_document
from datus_semantic_osi.profile import load_osi_path, to_core_schema_document
from datus_semantic_osi.validator import ensure_valid, validate_ir, validate_profile


def _cmd_compile(args: argparse.Namespace) -> int:
    try:
        doc = load_osi_path(args.input)
        ensure_valid(validate_profile(doc))
        model = compile_document(doc)
        ensure_valid(validate_ir(model))
    except OSIError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    if args.ir:
        Path(args.ir).write_text(
            model.model_dump_json(indent=2, exclude_defaults=False)
        )

    backend = make_backend(args.backend, generated_path=args.output)
    ensure_valid_caps = getattr(backend, "capabilities", {}) or {}
    from datus_semantic_osi.validator import validate_capabilities

    try:
        ensure_valid(validate_capabilities(model, ensure_valid_caps))
    except OSIError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    artifact = backend.lower(model)
    written = artifact.write(Path(args.output))
    print(
        f"compiled {len(model.metrics)} metric(s) into {args.backend} artifact at {args.output}"
    )
    for kind, path in written.items():
        print(f"  {kind}: {path}")
    return 0


def _cmd_migrate(args: argparse.Namespace) -> int:
    from datus_semantic_osi.migrate import migrate_metricflow_yaml

    osi_doc, report = migrate_metricflow_yaml(Path(args.input).read_text())
    Path(args.output).write_text(
        yaml.safe_dump(osi_doc, sort_keys=False, allow_unicode=True)
    )
    print(f"migrated MetricFlow YAML -> OSI at {args.output}")
    if report:
        print(f"{len(report)} item(s) need manual Datus hints:")
        for item in report:
            print(f"  - {item}")
    return 0


def _dump_osi_document(result: NormalizationResult) -> str:
    payload = to_core_schema_document(result.document)
    return yaml.safe_dump(
        payload,
        sort_keys=False,
        allow_unicode=True,
    )


def _osi_files(path: Path) -> List[Path]:
    if path.is_dir():
        return sorted([*path.glob("*.yaml"), *path.glob("*.yml")])
    return [path]


def _norm_name(value: object) -> str:
    return str(value or "").strip().lower()


def _merge_raw_dataset(raw: dict, normalized: object) -> dict:
    merged = dict(raw)
    if "primary_key" not in merged and normalized.primary_key is not None:
        merged["primary_key"] = normalized.primary_key
    if "time_dimension" not in merged and normalized.time_dimension is not None:
        merged["time_dimension"] = normalized.time_dimension.model_dump(
            mode="json", exclude_none=True
        )

    raw_dimensions = list(merged.get("dimensions") or [])
    seen = {
        _norm_name(dim.get("name"))
        for dim in raw_dimensions
        if isinstance(dim, dict) and dim.get("name")
    }
    for dim in normalized.dimensions:
        name = _norm_name(dim.name)
        if name and name not in seen:
            raw_dimensions.append(dim.model_dump(mode="json", exclude_none=True))
            seen.add(name)
    if raw_dimensions:
        merged["dimensions"] = raw_dimensions
    return merged


def _rewrite_raw_document(raw: object, result: NormalizationResult) -> object:
    if not isinstance(raw, dict):
        return raw

    aliases = result.dataset_aliases
    if not aliases:
        return raw

    rewritten = dict(raw)
    removed_datasets = set(aliases)
    canonical = {ds.name: ds for ds in result.document.datasets}

    datasets = rewritten.get("datasets")
    if isinstance(datasets, list):
        new_datasets = []
        for ds in datasets:
            if not isinstance(ds, dict):
                new_datasets.append(ds)
                continue
            name = ds.get("name")
            if name in removed_datasets:
                continue
            if name in canonical:
                new_datasets.append(_merge_raw_dataset(ds, canonical[name]))
            else:
                new_datasets.append(ds)
        if new_datasets:
            rewritten["datasets"] = new_datasets
        else:
            rewritten.pop("datasets", None)

    metrics = rewritten.get("metrics")
    if isinstance(metrics, list):
        for metric in metrics:
            if isinstance(metric, dict) and metric.get("dataset") in aliases:
                metric["dataset"] = aliases[metric["dataset"]]

    relationships = rewritten.get("relationships")
    if isinstance(relationships, list):
        new_relationships = []
        seen = set()
        for rel in relationships:
            if not isinstance(rel, dict):
                new_relationships.append(rel)
                continue
            rel = dict(rel)

            from_key = "from" if "from" in rel else "from_dataset"
            to_key = "to" if "to" in rel else "to_dataset"
            from_name = aliases.get(rel.get(from_key), rel.get(from_key))
            to_name = aliases.get(rel.get(to_key), rel.get(to_key))
            rel[from_key] = from_name
            rel[to_key] = to_name

            if from_name == to_name:
                continue
            key = (
                rel.get("name"),
                from_name,
                to_name,
                tuple(rel.get("from_columns") or [rel.get("from_identifier")]),
                tuple(rel.get("to_columns") or [rel.get("to_identifier")]),
            )
            if key in seen:
                continue
            seen.add(key)
            new_relationships.append(rel)
        if new_relationships:
            rewritten["relationships"] = new_relationships
        else:
            rewritten.pop("relationships", None)

    return rewritten


def _write_normalized(input_path: Path, result: NormalizationResult) -> None:
    if input_path.is_file():
        input_path.write_text(_dump_osi_document(result))
        return

    target = input_path / "semantic_model.normalized.yml"
    target.write_text(_dump_osi_document(result))
    for path in _osi_files(input_path):
        if path != target:
            path.write_text("")


def _cmd_normalize(args: argparse.Namespace) -> int:
    try:
        doc = load_osi_path(args.input)
        result = normalize_document(doc)
    except OSIError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    if result.errors:
        for error in result.errors:
            print(f"error: {error}", file=sys.stderr)
        return 1

    if result.actions:
        for action in result.actions:
            print(action)
    else:
        print("already normalized")

    if args.write and result.actions:
        _write_normalized(Path(args.input), result)
        print(f"wrote normalized OSI YAML under {args.input}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="datus-osi", description="Datus OSI semantic compiler"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    compile_p = sub.add_parser(
        "compile", help="Compile OSI authoring into a backend artifact"
    )
    compile_p.add_argument("--input", required=True, help="OSI YAML file or directory")
    compile_p.add_argument(
        "--output", required=True, help="Output directory for the backend artifact"
    )
    compile_p.add_argument(
        "--backend",
        default="metricflow",
        help="Execution backend (default: metricflow)",
    )
    compile_p.add_argument(
        "--ir", help="Optional path to also dump the Datus Semantic IR as JSON"
    )
    compile_p.set_defaults(func=_cmd_compile)

    migrate_p = sub.add_parser(
        "migrate", help="Best-effort migrate legacy MetricFlow YAML to OSI"
    )
    migrate_p.add_argument("--input", required=True, help="Legacy MetricFlow YAML file")
    migrate_p.add_argument("--output", required=True, help="Output OSI YAML file")
    migrate_p.set_defaults(func=_cmd_migrate)

    normalize_p = sub.add_parser(
        "normalize", help="Check or rewrite duplicate physical-table dataset aliases"
    )
    normalize_p.add_argument("--input", required=True, help="OSI YAML file or directory")
    mode = normalize_p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="Report normalization actions without writing")
    mode.add_argument("--write", action="store_true", help="Rewrite OSI YAML in place")
    normalize_p.set_defaults(func=_cmd_normalize)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
