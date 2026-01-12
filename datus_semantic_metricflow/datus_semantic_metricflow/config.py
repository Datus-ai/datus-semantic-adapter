from typing import Optional

from datus.tools.semantic_tools import SemanticAdapterConfig
from pydantic import Field


class MetricFlowConfig(SemanticAdapterConfig):
    """Configuration for MetricFlow adapter."""
    service_type: str = Field(default="metricflow", description="Service type")
    config_path: Optional[str] = Field(None, description="Path to MetricFlow configuration file")
    timeout: int = Field(default=300, description="Query timeout in seconds")

    class Config:
        extra = "allow"
