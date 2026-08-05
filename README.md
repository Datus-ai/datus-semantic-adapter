# Datus Semantic Adapter

Semantic layer adapters for the Datus platform.

This repository contains adapters that integrate various semantic layer backends with Datus, providing a unified interface for metric discovery, querying, and validation.

## Available Adapters

| Adapter | Package | Description |
|---------|---------|-------------|
| MetricFlow | `datus-semantic-metricflow` | MetricFlow semantic layer integration |
| OSI | `datus-semantic-osi` | OSI authoring and compilation to execution backends |
| Dosi | `datus-semantic-dosi` | Native OSI planning and execution through Dosi |

## Architecture

All adapters implement the `BaseSemanticAdapter` interface from
`datus-semantic-core`, providing:

- Metric listing and discovery
- Dimension querying
- Metric query execution
- Configuration validation

## Installation

Each adapter is published as a separate package:

```bash
pip install datus-semantic-metricflow
pip install datus-semantic-osi
pip install datus-semantic-dosi  # also installs dosi-engine
```

See individual adapter READMEs for detailed usage instructions.
