# datus-semantic-dosi

A Datus semantic adapter backed by [Dosi](https://github.com/datus-ai/osi-engine),
the native Rust OSI engine, with no MetricFlow dependency. It is a thin protocol
translator: the OSI YAML is loaded, planned, compiled to dialect SQL, and
executed entirely inside the Rust engine (via the `dosi-engine` pyo3
bindings); this package only maps the Datus semantic-adapter contract onto the
engine's API and its structured errors onto `SemanticValidationError`.

`service_type`: `dosi`.

## Install

The adapter declares the native engine as a normal dependency. One command
installs both packages:

```bash
pip install datus-semantic-dosi
```

No separate `dosi-engine` installation is required.

## Configure

```python
from datus_semantic_dosi.config import DosiConfig

DosiConfig(
    semantic_model_path="model.yaml",     # OSI model (.yaml/.yml/.json)
    db_config={"type": "duckdb", "uri": "orders.db"},  # or connections_path=...
)
```

Connection precedence: an explicit `connections_path` (agent.yml or a
standalone `datasources:` YAML, consumed verbatim by the engine) wins over an
inline `db_config` (one agent.yml datasource entry, written to a temporary
connections file). With neither, the engine falls back to its own discovery
order and, failing that, local DuckDB.

## Use with Datus-agent

Install the adapter into the same virtualenv as `datus-agent`:

```bash
uv pip install datus-semantic-dosi
```

For local development before a PyPI release, install the adapter checkout and a
locally built `dosi_engine-*.whl` together. Entry-point discovery requires an
installed distribution; `PYTHONPATH` alone is not sufficient.

Then wire it in `agent.yml`. The `semantic_layer` key **must equal the
`service_type`** (`dosi`); Datus-agent fills `db_config` from the active
datasource and `semantic_models_path` from `subject/semantic_models/<datasource>/`
automatically, so a model file dropped there needs no further config:

```yaml
agent:
  services:
    datasources:
      mydb:
        type: duckdb
        uri: /abs/path/to/orders.db
    semantic_layer:
      dosi:                 # key MUST be the service_type
        # both optional; either overrides the auto-derived directory:
        # semantic_model_path: /abs/path/to/model.yaml   # explicit single file
        # connections_path: /abs/path/to/agent.yml       # reuse a connections file
```

Place one OSI model file at `<project>/subject/semantic_models/mydb/model.yaml`
(Datus's per-datasource convention). The adapter resolves a single file in that
directory automatically; **if the directory holds several models, set
`semantic_model_path`** to pick one (the engine loads exactly one model per
document). Launch with `datus --datasource mydb`; the `ask_metrics` node then
drives `list_metrics` / `query_metrics` through this adapter.

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
`ci/run-unit-tests.sh datus-semantic-dosi`. Integration tests
(`-m integration`) need the real `dosi-engine` wheel and the `duckdb` CLI used
to seed the test fixture,
and use the vendored `tests/fixtures/orders/` copy.
