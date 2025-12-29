"""
Models for MetricFlow adapter.

Re-exports common models from datus.tools.semantic_tools.models
and defines adapter-specific extensions.
"""

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

# Re-export common models from datus-agent
from datus.tools.semantic_tools.models import (
    QueryResult,
    ValidationIssue,
    ValidationResult,
)

__all__ = [
    "MetricType",
    "MetricDefinition",
    "QueryResult",
    "ValidationIssue",
    "ValidationResult",
]


class MetricType(str, Enum):
    """Type of metric in MetricFlow."""

    SIMPLE = "simple"
    RATIO = "ratio"
    CUMULATIVE = "cumulative"
    DERIVED = "derived"


class MetricDefinition(BaseModel):
    """
    Definition of a metric.

    Extended from base to include MetricFlow-specific fields.
    """

    name: str = Field(..., description="Metric name")
    description: Optional[str] = Field(None, description="Metric description")
    type: Optional[MetricType] = Field(None, description="Metric type")
    dimensions: List[str] = Field(default_factory=list, description="Available dimensions")
    measures: List[str] = Field(default_factory=list, description="Underlying measures")
    path: Optional[List[str]] = Field(None, description="Subject area path")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
