# datus-semantic-dosi

A Datus semantic adapter backed by Dosi, the native Rust OSI engine, with no
MetricFlow dependency. It is a thin protocol translator: the OSI YAML is
loaded, planned, compiled to dialect SQL, and executed entirely inside the Rust
engine (via the `dosi-engine` pyo3 bindings); this package only maps the Datus
semantic-adapter contract onto the engine's API and its structured errors onto
`SemanticValidationError`.

`service_type`: `dosi`.

## Install

The adapter declares the native engine as a normal dependency. One command
installs both packages:

```bash
pip install datus-semantic-dosi
```

No separate `dosi-engine` installation is required.

For local source development, a release is not required. Install the native
checkout first, then the adapter into the same environment:

```bash
uv pip install -e ../osi-engine/crates/dosi-py
uv pip install -e ./datus-semantic-dosi
```

Adjust the relative paths to the workspace root. The first command builds the
Rust/PyO3 extension with maturin and keeps the Python package linked to the
local checkout.

## Configure

```python
from datus_semantic_dosi.config import DosiConfig

DosiConfig(
    semantic_model_path="model.yaml",  # OSI model (.yaml/.yml/.json)
    db_config={"type": "sqlite", "uri": "orders.sqlite"},  # or duckdb / connections_path
)
```

Connection precedence: an explicit `connections_path` (agent.yml or a
standalone `datasources:` YAML, consumed verbatim by the engine) wins over an
inline `db_config` (one agent.yml datasource entry, written to a temporary
connections file). With neither, the engine falls back to its own discovery
order and, failing that, local DuckDB.

SQLite is a storage connector rather than a SQL dialect: Dosi builds that
advertise SQLite connection support receive `type: sqlite` unchanged and
execute DuckDB SQL against the file in read-only mode. For compatibility,
older installed engine builds still use the adapter's cached DuckDB companion
bridge.

## Use with Datus-agent

Install the adapter into the same virtualenv as `datus-agent`:

```bash
uv pip install datus-semantic-dosi
```

For local development before a registry release, install both checkouts with
the editable commands above. Entry-point discovery requires an installed
adapter distribution; `PYTHONPATH` alone is not sufficient.

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

Place OSI model files under `<project>/subject/semantic_models/mydb/` (Datus's
per-datasource convention). The adapter catalogs every top-level YAML/YML/JSON
file, keeps one native engine per file, and routes each globally unique metric
name to its owning model. Set `semantic_model_path` only when an authoring flow
must pin the adapter to one explicit file. Launch with `datus --datasource
mydb`; the `ask_metrics` node then drives `list_metrics` / `query_metrics`
through this adapter.

## Behavior notes

- **`validate_semantic`** delegates to the engine's own validator (structure,
  references, metric compilation) — no separate ossie integration.
- **`get_dimensions(metric)`** checks model dimensions against that metric with
  native compile-only planning and returns only queryable candidates and
  grains. Window discovery includes the planner-required time axis while
  testing business dimensions. `list_metrics` leaves metric-level dimensions
  empty until the engine exposes this catalog relation directly.
- **Ambiguous / unknown names** surface as `SemanticValidationException` whose
  `payload` carries the engine's `candidates`; single-candidate fixes are
  turned into a concrete `suggested_retry`.
- **Multiple model files** are supported for discovery and single-model
  queries. Metric and semantic-model names must be unique within a datasource;
  one query cannot combine metrics owned by different files.
- **Time granularity** attaches only to time dimensions; supplying it with no
  time dimension raises a `time_grain_required` validation payload.
- **Native time axis** accepts `metric_time` plus `time_granularity`; a suffixed
  result column is an output/order key, not a query dimension.
- **Window discovery** exposes native structured-window metadata and rejects legacy
  `grain_to_date`, `window_aggregation`, `period_over_period`, and string
  `window` hints instead of silently executing the base aggregate.
- Engine instances and the metric routing catalog refresh when model files are
  added, removed, or changed.

## Tests

Unit tests run against a fake binding (no native build needed):
`ci/run-unit-tests.sh datus-semantic-dosi`. Integration tests
(`-m integration`) need the real local or installed `dosi-engine` binding and the `duckdb` CLI used
to seed the test fixture,
and use the vendored `tests/fixtures/orders/` copy.
