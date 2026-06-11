# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Best-effort migration: legacy MetricFlow YAML -> OSI authoring + a report.

Recovers business semantics from generated MetricFlow YAML so existing models can
adopt OSI. Structures that cannot be safely recovered (constraints, fanout,
non-additive dimensions, etc.) are listed in the migration report for a human to
re-declare as Datus hints rather than being silently dropped or mis-translated.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

import yaml

from datus_semantic_osi.profile import parse_osi_profile, to_core_schema_document

# MetricFlow measure agg -> SQL function template
_AGG_TEMPLATE = {
    "sum": "SUM({expr})",
    "count": "COUNT({expr})",
    "count_distinct": "COUNT(DISTINCT {expr})",
    "average": "AVG({expr})",
    "avg": "AVG({expr})",
    "min": "MIN({expr})",
    "max": "MAX({expr})",
}


def _measure_expression(measure: dict) -> str:
    agg = str(measure.get("agg", "")).lower()
    expr = measure.get("expr", measure.get("name"))
    template = _AGG_TEMPLATE.get(agg)
    if not template:
        return str(expr)
    return template.format(expr=expr)


def _load_docs(yaml_text: str) -> List[dict]:
    return [d for d in yaml.safe_load_all(yaml_text) if d]


def migrate_metricflow_yaml(yaml_text: str) -> Tuple[dict, List[str]]:
    """Convert legacy MetricFlow YAML into an OSI authoring dict + report.

    Returns a tuple ``(osi_document_dict, report)`` where report lists structures
    that need manual Datus hints.
    """
    docs = _load_docs(yaml_text)
    report: List[str] = []

    datasets: List[dict] = []
    # measure name -> (expression, owning dataset)
    measure_expr: Dict[str, str] = {}

    for doc in docs:
        ds = doc.get("data_source")
        if not ds:
            continue
        osi_ds: dict = {"name": ds["name"], "source": {}}
        if ds.get("sql_table"):
            osi_ds["source"]["table"] = ds["sql_table"]
        elif ds.get("sql_query"):
            osi_ds["source"]["query"] = ds["sql_query"]

        # identifiers -> primary_key
        primary = [i for i in ds.get("identifiers", []) if i.get("type") == "primary"]
        if primary:
            osi_ds["primary_key"] = primary[0].get("expr", primary[0]["name"])

        # dimensions -> time_dimension + categorical dimensions
        dims = []
        for dim in ds.get("dimensions", []):
            if dim.get("type") == "time":
                tp = dim.get("type_params", {})
                osi_ds["time_dimension"] = {
                    "name": dim["name"],
                    "granularity": tp.get("time_granularity", "day"),
                }
            else:
                entry = {"name": dim["name"]}
                if dim.get("expr"):
                    entry["expr"] = dim["expr"]
                dims.append(entry)
        if dims:
            osi_ds["dimensions"] = dims

        for measure in ds.get("measures", []):
            measure_expr[measure["name"]] = _measure_expression(measure)
            if measure.get("non_additive_dimension"):
                report.append(
                    f"measure `{measure['name']}` uses non_additive_dimension; "
                    "re-declare semi-additive semantics as a Datus hint."
                )
        datasets.append(osi_ds)

    metrics: List[dict] = []
    default_dataset = datasets[0]["name"] if datasets else None

    for doc in docs:
        mf = doc.get("metric")
        if not mf:
            continue
        name = mf["name"]
        mtype = str(mf.get("type", "")).lower()
        tp = mf.get("type_params", {}) or {}
        osi_metric: dict = {"name": name, "dataset": default_dataset}
        if mf.get("description"):
            osi_metric["description"] = mf["description"]

        if mtype == "measure_proxy":
            measures = tp.get("measures", [])
            if measures:
                osi_metric["expression"] = measure_expr.get(measures[0], measures[0])
        elif mtype == "ratio":
            osi_metric["metric_kind"] = "ratio"
            osi_metric["numerator"] = tp.get("numerator")
            osi_metric["denominator"] = tp.get("denominator")
        elif mtype == "expr":
            expr = tp.get("expr", "")
            for measure, sql in measure_expr.items():
                expr = expr.replace(measure, sql)
            osi_metric["expression"] = expr
        elif mtype == "cumulative":
            measures = tp.get("measures", [])
            if measures:
                osi_metric["expression"] = measure_expr.get(measures[0], measures[0])
            if tp.get("window"):
                osi_metric["window"] = tp["window"]
            if tp.get("grain_to_date"):
                osi_metric["grain_to_date"] = tp["grain_to_date"]
        elif mtype == "derived":
            osi_metric["metric_kind"] = "derived"
            osi_metric["expression"] = tp.get("expr", "")
            inputs = []
            for m in tp.get("metrics", []):
                inp = {"name": m["name"]}
                if m.get("alias"):
                    inp["alias"] = m["alias"]
                if m.get("offset_window"):
                    inp["offset_window"] = m["offset_window"]
                inputs.append(inp)
            if inputs:
                osi_metric["inputs"] = inputs
        else:
            report.append(
                f"metric `{name}` has unsupported type `{mtype}`; migrate manually."
            )
            continue

        if mf.get("constraint"):
            report.append(
                f"metric `{name}` has a MetricFlow constraint; re-declare it as a Datus "
                "filter hint (scope: metric or measure) on the OSI metric."
            )
        metrics.append(osi_metric)

    profile_doc = {
        "semantic_model": {"name": "migrated_model"},
        "datasets": datasets,
        "metrics": metrics,
    }
    return to_core_schema_document(parse_osi_profile(profile_doc)), report
