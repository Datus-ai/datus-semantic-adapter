# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Condensed OSI core authoring skeleton for LLM prompts.

The host agent injects this into semantic-authoring prompts so the authoring
rules and the schema the adapter validates against ship from the same package
and cannot drift. The skeleton is the executable subset: the full spec's
dialect enum is collapsed to the single configured dialect, and Datus
execution hints are shown where the spec has no core field for them.
"""

from __future__ import annotations

from datus_semantic_osi.profile import CORE_SCHEMA_VERSION

_SKELETON_TEMPLATE = """\
# OSI core authoring skeleton (schema version {version}; expression dialect: {dialect})
version: {version}
semantic_model:
  - name: <model_name>
    datasets:
      - name: <dataset_name>
        source: <db.schema.table or SQL query>   # query sources: add DATUS hint {{"source_type": "query"}}
        description: <business meaning>
        ai_context: <string, or {{instructions, synonyms, examples}}>
        primary_key: [<col>, ...]                # optional; transcribe declared keys only, never guess
        unique_keys:                             # optional; each entry is one unique key
          - [<col>]
        fields:
          # Every business-meaningful column is a field. The `dimension:` block
          # is the opt-in for grouping/filtering: grouping attributes carry it,
          # aggregation-only columns (balances, amounts, rates) OMIT it.
          - name: <grouping_or_filter_column>
            expression:
              dialects:
                - dialect: {dialect}
                  expression: <scalar SQL, e.g. the column name>
            dimension: {{}}                        # presence of this block = usable for grouping/filtering
            description: <business meaning>
          - name: <aggregation_only_column>
            expression:
              dialects:
                - dialect: {dialect}
                  expression: <scalar SQL>
            description: <business meaning>       # no dimension block: row-level measure source
          - name: <time_column>
            expression:
              dialects:
                - dialect: {dialect}
                  expression: <time column>
            dimension:
              is_time: true                      # the dataset's time dimension
            custom_extensions:
              - vendor_name: DATUS
                data: '{{"time_granularity": "day|week|month|quarter|year"}}'
    relationships:                               # optional; many-side first
      - name: <fact>_to_<dim>
        from: <many_side_dataset>
        to: <one_side_dataset>
        from_columns: [<fk_col>]
        to_columns: [<pk_col>]
    metrics:
      - name: <metric_name>
        description: <business meaning>
        ai_context: <string or object>
        expression:
          dialects:
            - dialect: {dialect}
              expression: <aggregate SQL, e.g. SUM(<dataset>.<col>)>
        custom_extensions:                       # Datus execution hints (only keys the spec lacks)
          - vendor_name: DATUS
            data: '{{"time_dimension": "<time_col>", "subject_path": ["..."], "format": "0.00", "unit": "..."}}'
"""


def authoring_spec_skeleton(dialect: str) -> str:
    """Render the OSI core authoring skeleton for the configured dialect."""
    return _SKELETON_TEMPLATE.format(version=CORE_SCHEMA_VERSION, dialect=dialect)
