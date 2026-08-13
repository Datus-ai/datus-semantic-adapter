# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Prompt-facing OSI core and DATUS extension authoring specifications."""

from __future__ import annotations

import re
from importlib import resources

_SPEC_RESOURCE = "osi-core-0.2.0.dev0.spec.yaml"
_DATUS_SPEC_RESOURCE = "datus-extensions-{version}.spec.yaml"
_SPEC_TITLE_MARKER = "# Apache Ossie - Core Metadata Spec"
_DIALECTS_BLOCK_RE = re.compile(
    r"(# Supported expression language dialects\ndialects:\n)"
    r'(?:  - "[^"]+"[^\n]*\n)+',
)

_NATIVE_NOTES = """\

---
# Dosi native authoring notes
# - Keep exactly one semantic_model object per file; Dosi does not merge model
#   fragments before validation or execution.
# - Every expression dialect must be `{dialect}` for this datasource.
# - Dosi execution metadata belongs in vendor_name: DATUS custom_extensions.
#   Use the active DATUS extension authoring specification for supported keys.
# - The native Dosi parser/compiler is authoritative after every mutation.
"""


def authoring_spec_text(dialect: str) -> str:
    """Render the vendored OSI core spec for the active SQL dialect."""

    raw = (
        resources.files("datus_semantic_dosi.schema")
        .joinpath(_SPEC_RESOURCE)
        .read_text(encoding="utf-8")
    )
    title_at = raw.find(_SPEC_TITLE_MARKER)
    if title_at >= 0:
        raw = raw[title_at:]
    replacement = (
        "# Supported expression language dialects\n"
        "dialects:\n"
        f'  - "{dialect}"              # the only dialect executed in this deployment\n'
    )
    return _DIALECTS_BLOCK_RE.sub(replacement, raw, count=1) + _NATIVE_NOTES.format(
        dialect=dialect
    )


def datus_extension_authoring_spec_text(dialect: str) -> str:
    """Render the extension spec matching the active native engine version."""

    from .engine import datus_extension_version

    version = datus_extension_version()
    resource = _DATUS_SPEC_RESOURCE.format(version=version)
    spec_path = resources.files("datus_semantic_dosi.schema").joinpath(resource)
    if not spec_path.is_file():
        raise RuntimeError(
            "DATUS extension authoring specification is unavailable for "
            f"engine version {version}"
        )
    return spec_path.read_text(encoding="utf-8").replace("__OSI_DIALECT__", dialect)
