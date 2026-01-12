from typing import Optional

from datus.tools.semantic_tools import SemanticAdapterConfig
from pydantic import Field


class MetricFlowConfig(SemanticAdapterConfig):
    """Configuration for MetricFlow adapter."""
    service_type: str = Field(default="metricflow", description="Service type")
    cli_path: str = Field(default="mf", description="Path to MetricFlow CLI executable")
    project_root: Optional[str] = Field(None, description="Root directory of MetricFlow project")
    environment: Optional[str] = Field(None, description="Environment name (if using profiles)")
    timeout: int = Field(default=300, description="Command timeout in seconds")

    class Config:
        extra = "allow"
