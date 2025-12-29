from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class TimeGranularity(str, Enum):
    """Time granularity for metric queries."""
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class TimeRange(BaseModel):
    """Time range specification for queries."""
    start: Optional[str] = Field(None, description="Start time (ISO format or natural language)")
    end: Optional[str] = Field(None, description="End time (ISO format or natural language)")
    granularity: Optional[TimeGranularity] = Field(None, description="Time granularity")


class MetricType(str, Enum):
    """Type of metric."""
    SIMPLE = "simple"
    RATIO = "ratio"
    CUMULATIVE = "cumulative"
    DERIVED = "derived"


class MetricDefinition(BaseModel):
    """Definition of a metric."""
    name: str = Field(..., description="Metric name")
    description: Optional[str] = Field(None, description="Metric description")
    type: Optional[MetricType] = Field(None, description="Metric type")
    dimensions: List[str] = Field(default_factory=list, description="Available dimensions")
    measures: List[str] = Field(default_factory=list, description="Underlying measures")
    path: Optional[List[str]] = Field(None, description="Subject area path")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class QueryResult(BaseModel):
    """Result of a metric query."""
    columns: List[str] = Field(..., description="Column names")
    data: List[List[Any]] = Field(..., description="Result data rows")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Query metadata (SQL, etc.)")


class ValidationIssue(BaseModel):
    """A single validation issue."""
    severity: str = Field(..., description="Severity level: error, warning, info")
    message: str = Field(..., description="Issue description")
    location: Optional[str] = Field(None, description="Location in config where issue was found")


class ValidationResult(BaseModel):
    """Result of semantic layer validation."""
    valid: bool = Field(..., description="Whether validation passed")
    issues: List[ValidationIssue] = Field(default_factory=list, description="List of validation issues")
