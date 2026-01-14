"""
Models for MetricFlow adapter.

Re-exports common models from datus.tools.semantic_tools.models.
"""

# Re-export common models from datus-agent
from datus.tools.semantic_tools.models import (
    DimensionInfo,
    MetricDefinition,
    QueryResult,
    ValidationIssue,
    ValidationResult,
)

# Import MetricType directly from metricflow
from metricflow.model.objects.metric import MetricType

__all__ = [
    "MetricType",
    "MetricDefinition",
    "DimensionInfo",
    "QueryResult",
    "ValidationIssue",
    "ValidationResult",
]
