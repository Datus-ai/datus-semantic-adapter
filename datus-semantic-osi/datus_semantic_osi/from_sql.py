# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Infer an OSI metric from a SQL query, choosing the correct anchor (grain).

A metric must be anchored on the table whose rows are being aggregated; other
joined tables become many-to-one dimension sources. Joining from the aggregated
("one") side to a "many" side would fan out the grain and silently inflate the
result, so such queries are rejected. Cardinality is derived from primary keys;
when it (or the grain) is ambiguous, a business-semantic error is raised instead
of guessing.
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, List, Optional, Tuple

import sqlglot
from sqlglot import expressions as exp

from datus_semantic_osi.errors import OSIValidationError
from datus_semantic_osi.profile import parse_osi_profile, to_core_schema_document


def _norm(name: Optional[str]) -> str:
    return (name or "").lower()


def _table_map(select: exp.Select) -> Dict[str, str]:
    """alias-or-name -> real table name, for every table in FROM + JOINs."""
    mapping: Dict[str, str] = {}
    for tbl in select.find_all(exp.Table):
        real = tbl.name
        mapping[_norm(real)] = real
        if tbl.alias:
            mapping[_norm(tbl.alias)] = real
    return mapping


def _column_table(
    col: exp.Column, tables: Dict[str, str], all_real: List[str]
) -> Optional[str]:
    """Resolve which real table a column belongs to."""
    if col.table:
        return tables.get(_norm(col.table))
    # unqualified: only safe when there is exactly one table
    return all_real[0] if len(set(all_real)) == 1 else None


def _aggregate_projection(select: exp.Select):
    """Return the single aggregate projection (alias, expr) or raise."""
    aggs = [p for p in select.expressions if p.find(exp.AggFunc)]
    if not aggs:
        raise OSIValidationError(
            "query has no aggregate; it is a detail/list query, not a metric.",
            hint="Model detail queries as a dataset/view, not a metric.",
        )
    proj = aggs[0]
    name = proj.alias_or_name or None
    body = proj.this if isinstance(proj, exp.Alias) else proj
    return name, body


def _primary_keys_of(table: str, primary_keys: Dict[str, Iterable[str]]) -> set:
    for k, cols in primary_keys.items():
        if _norm(k) == _norm(table):
            return {_norm(c) for c in cols}
    return set()


def _join_pairs(on: exp.Expression) -> List[Tuple[exp.Column, exp.Column]]:
    pairs = []
    for eq in on.find_all(exp.EQ):
        left, right = eq.this, eq.expression
        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
            pairs.append((left, right))
    return pairs


def infer_metric_from_sql(
    sql: str,
    *,
    primary_keys: Optional[Dict[str, Iterable[str]]] = None,
    time_dimensions: Optional[Dict[str, str]] = None,
    metric_name: Optional[str] = None,
    dialect: str = "mysql",
) -> dict:
    """Infer an OSI authoring document (datasets/relationships/metric) from SQL.

    Raises :class:`OSIValidationError` (business-facing) when the grain or the
    join cardinality is ambiguous, or when the join would fan out the anchor.
    """
    primary_keys = primary_keys or {}
    time_dimensions = time_dimensions or {}
    try:
        tree = sqlglot.parse_one(sql, read=dialect)
    except Exception as e:  # noqa: BLE001
        raise OSIValidationError(f"could not parse SQL: {e}") from e

    select = tree.find(exp.Select)
    if select is None:
        raise OSIValidationError("not a SELECT query.")
    if select.find(exp.Window):
        raise OSIValidationError(
            "uses a window/ranking function; model as a precomputed dataset/view, not a metric."
        )

    tables = _table_map(select)
    real_tables = []
    for tbl in select.find_all(exp.Table):
        if tbl.name not in real_tables:
            real_tables.append(tbl.name)

    name, agg_body = _aggregate_projection(select)

    # COUNT(*) over a join: the grain is the joined row, which is ambiguous.
    star_count = any(
        isinstance(c, exp.Count) and (isinstance(c.this, exp.Star) or c.this is None)
        for c in agg_body.find_all(exp.Count)
    )
    if star_count and len(real_tables) > 1:
        raise OSIValidationError(
            "uses COUNT(*) across a join; the counted grain is ambiguous.",
            hint="Count a specific entity instead, e.g. COUNT(DISTINCT <table>.<primary_key>).",
        )

    # Anchor = the table owning the aggregated column(s).
    agg_cols = list(agg_body.find_all(exp.Column))
    anchor_tables = {_column_table(c, tables, real_tables) for c in agg_cols}
    anchor_tables.discard(None)
    if len(anchor_tables) > 1:
        raise OSIValidationError(
            "aggregates columns from multiple tables; the metric grain is ambiguous.",
            hint="Anchor the metric on a single entity and join the rest as dimensions.",
        )
    if not anchor_tables:
        if len(real_tables) == 1:
            anchor = real_tables[0]
        else:
            raise OSIValidationError(
                "cannot determine which table the metric is anchored on.",
                hint="Reference the aggregated column with its table, e.g. SUM(orders.amount).",
            )
    else:
        anchor = anchor_tables.pop()

    # Orient each join edge as many_to_one (from many -> to one) using PKs.
    relationships: List[dict] = []
    dim_dims: Dict[str, List[dict]] = {}
    for join in select.args.get("joins") or []:
        on = join.args.get("on")
        if on is None:
            raise OSIValidationError(
                "join without an ON condition; cannot infer cardinality."
            )
        for left, right in _join_pairs(on):
            lt = _column_table(left, tables, real_tables)
            rt = _column_table(right, tables, real_tables)
            if not lt or not rt or _norm(lt) == _norm(rt):
                continue
            left_pk = _norm(left.name) in _primary_keys_of(lt, primary_keys)
            right_pk = _norm(right.name) in _primary_keys_of(rt, primary_keys)
            if not left_pk and not right_pk:
                raise OSIValidationError(
                    f"cannot determine join cardinality between `{lt}` and `{rt}`: "
                    "neither join column is a known primary key.",
                    hint="Provide primary-key metadata, or join on a unique key.",
                )
            # the side whose join column is its PK is the "one" side
            if right_pk and not left_pk:
                many, many_col, one, one_col = lt, left.name, rt, right.name
            elif left_pk and not right_pk:
                many, many_col, one, one_col = rt, right.name, lt, left.name
            else:  # both PK -> one-to-one; orient toward a non-anchor as the "one"
                if _norm(lt) == _norm(anchor):
                    many, many_col, one, one_col = lt, left.name, rt, right.name
                else:
                    many, many_col, one, one_col = rt, right.name, lt, left.name
            # fanout guard: the anchor must never be on the "one" side of a join
            if _norm(one) == _norm(anchor):
                raise OSIValidationError(
                    f"metric is anchored on `{anchor}` (the one side) but joins `{many}` "
                    "(the many side); this fan-out would inflate the aggregate.",
                    hint=f"Anchor the metric on `{many}` instead, or pre-aggregate `{many}`.",
                )
            relationships.append(
                {
                    "name": f"{many}_to_{one}",
                    "from": many,
                    "to": one,
                    "from_columns": [many_col],
                    "to_columns": [one_col],
                }
            )

    # GROUP BY columns from non-anchor tables -> dimensions on their datasets.
    for grp in (select.args.get("group") or exp.Group()).expressions:
        for col in grp.find_all(exp.Column):
            t = _column_table(col, tables, real_tables)
            if t and _norm(t) != _norm(anchor):
                dim_dims.setdefault(t, [])
                if not any(d["name"] == col.name for d in dim_dims[t]):
                    dim_dims[t].append({"name": col.name, "expr": col.name})

    # Build datasets: anchor + every joined table.
    datasets = []
    for t in real_tables:
        ds: dict = {"name": t, "source": {"table": t}}
        pk = _primary_keys_of(t, primary_keys)
        if pk:
            ds["primary_key"] = sorted(pk)[0] if len(pk) == 1 else sorted(pk)
        time_col = next(
            (c for k, c in time_dimensions.items() if _norm(k) == _norm(t)), None
        )
        if time_col:
            ds["time_dimension"] = {"name": time_col, "granularity": "day"}
        if t in dim_dims:
            ds["dimensions"] = dim_dims[t]
        datasets.append(ds)

    # Anchor metric: strip the table qualifier so the expression is anchor-local.
    expr_sql = agg_body.sql(dialect=dialect)
    expr_sql = re.sub(r"\b[A-Za-z_][\w]*\.", "", expr_sql)

    metric = {
        "name": metric_name or name or "metric",
        "expression": expr_sql,
        "dataset": anchor,
    }

    profile_doc: dict = {
        "semantic_model": {"name": f"{anchor}_model"},
        "datasets": datasets,
        "metrics": [metric],
    }
    if relationships:
        profile_doc["relationships"] = relationships
    return to_core_schema_document(parse_osi_profile(profile_doc))
