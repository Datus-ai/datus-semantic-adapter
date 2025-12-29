# datus-semantic-metricflow

MetricFlow adapter for Datus semantic layer.

This package provides a unified semantic layer abstraction for MetricFlow, following the adapter pattern. It allows Datus agents to query metrics, dimensions, and execute semantic queries through MetricFlow's CLI interface.

## Features

- **Metric Discovery**: List and search available metrics
- **Dimension Querying**: Get dimensions for specific metrics
- **Metric Queries**: Execute metric queries with filtering, grouping, and time ranges
- **Validation**: Validate MetricFlow semantic layer configuration
- **Async Support**: Full async/await support for all metric operations

## Installation

```bash
# Install from source
pip install -e .

# Or install from PyPI (when published)
pip install datus-semantic-metricflow
```

## Requirements

- Python >= 3.12
- dbt-metricflow >= 0.5.0 (provides the `mf` CLI)
- pydantic >= 2.0.0

## Quick Start

### 1. Basic Usage

```python
import asyncio
from datus_semantic_metricflow import MetricFlowAdapter, MetricFlowConfig

# Configure the adapter
config = MetricFlowConfig(
    namespace="my_project",
    project_root="/path/to/metricflow/project",
    cli_path="mf",  # or full path to mf executable
)

# Create adapter instance
adapter = MetricFlowAdapter(config)

# List available metrics
async def main():
    metrics = await adapter.list_metrics(limit=10)
    for metric in metrics:
        print(f"{metric.name}: {metric.description}")

    # Get dimensions for a metric
    dimensions = await adapter.get_dimensions("revenue")
    print(f"Dimensions: {dimensions}")

    # Query metrics
    result = await adapter.query_metrics(
        metrics=["revenue", "orders"],
        dimensions=["date", "region"],
        limit=100,
    )
    print(f"Columns: {result.columns}")
    print(f"Data: {result.data[:5]}")  # First 5 rows

asyncio.run(main())
```

### 2. Using the Registry

```python
from datus_semantic_metricflow import semantic_adapter_registry, MetricFlowConfig

# The adapter is auto-registered on import
adapter = semantic_adapter_registry.create_adapter(
    service_type="metricflow",
    config=MetricFlowConfig(namespace="my_project"),
)

# List all registered adapters
print(semantic_adapter_registry.list_adapters())
# Output: {'metricflow': 'MetricFlow'}
```

### 3. Query with Time Ranges

```python
from datus_semantic_metricflow import TimeRange, TimeGranularity

async def query_with_time():
    result = await adapter.query_metrics(
        metrics=["revenue"],
        dimensions=["date"],
        time_range=TimeRange(
            start="2024-01-01",
            end="2024-12-31",
            granularity=TimeGranularity.MONTH,
        ),
    )
    return result

result = asyncio.run(query_with_time())
```

### 4. Validation

```python
async def validate():
    validation = await adapter.validate_semantic()
    if validation.valid:
        print("Configuration is valid")
    else:
        for issue in validation.issues:
            print(f"{issue.severity}: {issue.message}")

asyncio.run(validate())
```

## Configuration

### MetricFlowConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `namespace` | str | Required | Namespace for this semantic layer instance |
| `cli_path` | str | `"mf"` | Path to MetricFlow CLI executable |
| `project_root` | str | None | Root directory of MetricFlow project |
| `environment` | str | None | Environment name (if using profiles) |
| `timeout` | int | 300 | Command timeout in seconds |

## API Reference

### MetricFlowAdapter

#### Methods

##### `list_metrics(path=None, limit=100, offset=0)`

List available metrics.

**Parameters:**
- `path` (List[str], optional): Subject area path filter
- `limit` (int): Maximum number of metrics to return
- `offset` (int): Number of metrics to skip

**Returns:** `List[MetricDefinition]`

##### `get_dimensions(metric_name, path=None)`

Get available dimensions for a metric.

**Parameters:**
- `metric_name` (str): Name of the metric
- `path` (List[str], optional): Subject area filter

**Returns:** `List[str]`

##### `query_metrics(metrics, dimensions=[], path=None, time_range=None, where=None, limit=None, order_by=None, dry_run=False)`

Query metrics with filtering and grouping.

**Parameters:**
- `metrics` (List[str]): List of metric names to query
- `dimensions` (List[str]): List of dimensions to group by
- `path` (List[str], optional): Subject area filter
- `time_range` (TimeRange, optional): Time range filter
- `where` (str, optional): WHERE clause filter
- `limit` (int, optional): Maximum number of rows
- `order_by` (List[str], optional): Columns to order by
- `dry_run` (bool): If True, explain query instead of executing

**Returns:** `QueryResult`

##### `validate_semantic()`

Validate semantic layer configuration.

**Returns:** `ValidationResult`

## Data Models

### MetricDefinition

```python
class MetricDefinition(BaseModel):
    name: str
    description: Optional[str]
    type: Optional[MetricType]
    dimensions: List[str]
    measures: List[str]
    path: Optional[List[str]]
    metadata: Dict[str, Any]
```

### QueryResult

```python
class QueryResult(BaseModel):
    columns: List[str]
    data: List[List[Any]]
    metadata: Dict[str, Any]
```

### TimeRange

```python
class TimeRange(BaseModel):
    start: Optional[str]
    end: Optional[str]
    granularity: Optional[TimeGranularity]
```

### TimeGranularity

```python
class TimeGranularity(str, Enum):
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"
```

## Development

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd datus-semantic-adapter

# Install in development mode with dev dependencies
pip install -e ".[dev]"
```

### Running Tests

```bash
pytest
```

### Code Formatting

```bash
# Format code
black datus_semantic_metricflow tests

# Lint code
ruff check datus_semantic_metricflow tests
```

## Integration with Datus

This adapter is designed to be automatically discovered by Datus through Python entry points. Once installed, it will be available for use in Datus agents.

```python
# In Datus agent code
from datus.tools.func_tool import SemanticFuncTool

semantic_tool = SemanticFuncTool(
    agent_config=agent_config,
    sub_agent_name="gen_metrics",
    adapter_type="metricflow",
)

tools = semantic_tool.available_tools()
```

## Architecture

This adapter implements the `BaseSemanticAdapter` interface, which defines:

- **Semantic Model Interface**: Optional methods for semantic model discovery
- **Metrics Interface**: Required async methods for metric operations
- **Storage Sync Interface**: Methods to sync data to unified storage

The adapter communicates with MetricFlow through its CLI interface, parsing command output and translating it into standardized data models.

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]
