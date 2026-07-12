# datus-semantic-osi-engine

A Datus semantic adapter backed by [osi-engine](https://github.com/datus-ai/osi-engine),
the native Rust OSI engine — no MetricFlow dependency. It is a thin protocol
translator: the OSI YAML is loaded, planned, compiled to dialect SQL, and
executed entirely inside the Rust engine (via the `datus-osi-engine` pyo3
bindings); this package only maps the Datus semantic-adapter contract onto the
engine's API and its structured errors onto `SemanticValidationError`.

`service_type`: `osi_engine`.

## Install

The Rust bindings are an optional extra so the package can be imported (and its
entry point discovered) without the compiled wheel:

```bash
pip install 'datus-semantic-osi-engine[engine]'
```

Without the `[engine]` extra, adapter use raises a `SemanticCoreException` with
an install hint. DuckDB execution additionally needs the system `duckdb` CLI.

## Configure

```python
from datus_semantic_osi_engine.config import OSIEngineConfig

OSIEngineConfig(
    semantic_model_path="model.yaml",     # OSI model (.yaml/.yml/.json)
    db_config={"type": "duckdb", "uri": "orders.db"},  # or connections_path=...
)
```

Connection precedence: an explicit `connections_path` (agent.yml or a
standalone `datasources:` YAML, consumed verbatim by the engine) wins over an
inline `db_config` (one agent.yml datasource entry, written to a temporary
connections file). With neither, the engine falls back to its own discovery
order and, failing that, local DuckDB.

## Behavior notes

- **`validate_semantic`** delegates to the engine's own validator (structure,
  references, metric compilation) — no separate ossie integration.
- **`get_dimensions(metric)`** returns every dimension in the model (v1):
  relationship-reachable dimensions are genuinely queryable, and the planner
  rejects invalid combinations with structured, retryable errors.
- **Ambiguous / unknown names** surface as `SemanticValidationException` whose
  `payload` carries the engine's `candidates`; single-candidate fixes are
  turned into a concrete `suggested_retry`.
- **Time granularity** attaches only to time dimensions; supplying it with no
  time dimension raises a `time_grain_required` validation payload.
- The engine instance is rebuilt when the model file's mtime changes.

## Tests

Unit tests run against a fake binding (no wheel needed):
`ci/run-unit-tests.sh datus-semantic-osi-engine`. Integration tests
(`-m integration`) need the real `datus-osi-engine` wheel and the `duckdb` CLI,
and use the vendored `tests/fixtures/orders/` copy.
