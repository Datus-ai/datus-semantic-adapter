# Datus Semantic MetricFlow Adapter

MetricFlow adapter for Datus semantic layer.

## Prerequisites

This adapter requires the Datus platform to be installed first:

```bash
pip install datus  # or your datus installation method
```

## Installation

This package includes MetricFlow as a Git submodule. Follow these steps to install:

### Option 1: Using the install script (Recommended)

```bash
# Clone with submodules
git clone --recursive https://github.com/your-org/datus-semantic-adapter.git
cd datus-semantic-adapter/datus_semantic_metricflow

# Run install script
./install.sh
```

### Option 2: Manual installation

```bash
# Clone with submodules
git clone --recursive https://github.com/your-org/datus-semantic-adapter.git
cd datus-semantic-adapter/datus_semantic_metricflow

# Install metricflow first
pip install -e metricflow

# Install this package
pip install -e .
```

### If you already cloned without --recursive

```bash
# Initialize submodules
git submodule update --init --recursive

# Then follow Option 1 or 2 above
```

## Usage

```python
from datus_semantic_metricflow import MetricFlowAdapter, MetricFlowConfig

# Configure adapter
config = MetricFlowConfig(
    namespace="your_namespace",
)

# Create adapter instance
adapter = MetricFlowAdapter(config)

# List metrics
metrics = await adapter.list_metrics()

# Query metrics
result = await adapter.query_metrics(
    metrics=["revenue"],
    dimensions=["metric_time"],
    limit=10
)
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black .
ruff check .
```
