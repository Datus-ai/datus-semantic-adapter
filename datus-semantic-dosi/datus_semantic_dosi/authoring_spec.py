# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""Prompt-facing OSI core and DATUS extension authoring specifications."""

from __future__ import annotations

import re
from hashlib import sha256
from importlib import resources

from datus_semantic_core.exceptions import SemanticCoreException

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
    rendered, substitutions = _DIALECTS_BLOCK_RE.subn(replacement, raw, count=1)
    if substitutions != 1:
        raise SemanticCoreException(
            "the vendored OSI core spec no longer exposes a recognizable "
            f"dialects block; re-check {_SPEC_RESOURCE!r} after updating it"
        )
    return rendered + _NATIVE_NOTES.format(dialect=dialect)


def _legacy_datus_extension_authoring_spec_text(dialect: str) -> str:
    """Render the adapter-vendored spec used by older engine bindings."""

    from .engine import datus_extension_version

    version = datus_extension_version()
    resource = _DATUS_SPEC_RESOURCE.format(version=version)
    spec_path = resources.files("datus_semantic_dosi.schema").joinpath(resource)
    if not spec_path.is_file():
        raise SemanticCoreException(
            "DATUS extension authoring specification is unavailable for "
            f"engine version {version!r}; vendor {resource!r} or install a "
            "matching dosi-engine build"
        )
    return spec_path.read_text(encoding="utf-8").replace("__OSI_DIALECT__", dialect)


def datus_extension_authoring_spec_text(dialect: str = "<osi_dialect>") -> str:
    """Return the active engine's dialect-neutral DATUS authoring contract.

    ``dialect`` remains accepted for compatibility with callers using an older
    adapter API. Current engines own this contract and do not interpolate SQL
    dialect into it; the agent supplies the active dialect separately.
    """

    from .engine import load_binding

    renderer = getattr(load_binding(), "render_datus_authoring_spec", None)
    if callable(renderer):
        rendered = renderer()
        if not isinstance(rendered, str) or not rendered.strip():
            raise SemanticCoreException(
                "the installed dosi-engine returned an empty DATUS authoring contract"
            )
        return rendered
    return _legacy_datus_extension_authoring_spec_text(dialect)


def datus_extension_authoring_spec_digest() -> str:
    """Return a cache key for the active engine's authoring contract."""

    from .engine import datus_authoring_contract_digest, load_binding

    if callable(getattr(load_binding(), "render_datus_authoring_spec", None)):
        return datus_authoring_contract_digest()

    legacy = _legacy_datus_extension_authoring_spec_text("__OSI_DIALECT__")
    return f"sha256:{sha256(legacy.encode('utf-8')).hexdigest()}"
