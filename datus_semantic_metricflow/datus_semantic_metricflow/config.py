from typing import Optional
from pydantic import BaseModel, Field


class SemanticAdapterConfig(BaseModel):
    """Base configuration for semantic adapters."""
    namespace: str = Field(..., description="Namespace for this semantic layer instance")
    service_type: str = Field(default="metricflow", description="Type of semantic service")


class MetricFlowConfig(SemanticAdapterConfig):
    """Configuration for MetricFlow adapter."""
    service_type: str = Field(default="metricflow", description="Service type")
    cli_path: str = Field(default="mf", description="Path to MetricFlow CLI executable")
    project_root: Optional[str] = Field(None, description="Root directory of MetricFlow project")
    environment: Optional[str] = Field(None, description="Environment name (if using profiles)")
    timeout: int = Field(default=300, description="Command timeout in seconds")

    class Config:
        extra = "allow"
