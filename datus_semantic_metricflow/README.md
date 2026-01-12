# datus-semantic-metricflow

MetricFlow adapter for Datus semantic layer.

## Installation

```bash
pip install datus-semantic-metricflow
```

Dependencies (`datus-metricflow`, `pydantic`) will be installed automatically.

## Requirements

- Python >= 3.12

## Quick Start

```python
import asyncio
from datus_semantic_metricflow import MetricFlowAdapter, MetricFlowConfig

config = MetricFlowConfig(
    namespace="my_project",
    config_path="/path/to/metricflow/config",  # optional
)

adapter = MetricFlowAdapter(config)

async def main():
    # List metrics
    metrics = await adapter.list_metrics(limit=10)
    for metric in metrics:
        print(f"{metric.name}: {metric.description}")

    # Get dimensions for a metric
    dimensions = await adapter.get_dimensions("revenue")
    for dim in dimensions:
        print(f"{dim.name}: {dim.description}")

    # Query metrics
    result = await adapter.query_metrics(
        metrics=["revenue", "orders"],
        dimensions=["date", "region"],
        limit=100,
    )
    print(f"Columns: {result.columns}")
    print(f"Data: {result.data[:5]}")

asyncio.run(main())
```

## Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `namespace` | str | Required | Namespace for this semantic layer instance |
| `config_path` | str | None | Path to MetricFlow configuration file |
| `timeout` | int | 300 | Query timeout in seconds |

## API

- `list_metrics(path=None, limit=100, offset=0)` - List available metrics
- `get_dimensions(metric_name, path=None)` - Get dimensions for a metric
- `query_metrics(metrics, dimensions=[], ...)` - Execute metric queries
- `validate_semantic()` - Validate configuration files

## Development

```bash
pip install -e ".[dev]"
pytest
```
