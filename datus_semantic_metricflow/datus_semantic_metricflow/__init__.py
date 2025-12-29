from datus.tools.semantic_tools.base import BaseSemanticAdapter
from datus_semantic_metricflow.adapter import MetricFlowAdapter
from datus_semantic_metricflow.config import MetricFlowConfig, SemanticAdapterConfig
from datus_semantic_metricflow.models import (
    MetricDefinition,
    MetricType,
    QueryResult,
    ValidationIssue,
    ValidationResult,
)


def register():
    """
    Register MetricFlow adapter with Datus semantic adapter registry.

    This function is called via entry_point by Datus when discovering adapters.
    """
    # Import Datus registry at runtime to avoid circular dependencies
    from datus.tools.semantic_tools.registry import semantic_adapter_registry

    semantic_adapter_registry.register(
        service_type="metricflow",
        adapter_class=MetricFlowAdapter,
        config_class=MetricFlowConfig,
        display_name="MetricFlow",
    )


__all__ = [
    "MetricFlowAdapter",
    "BaseSemanticAdapter",
    "MetricFlowConfig",
    "SemanticAdapterConfig",
    "MetricDefinition",
    "MetricType",
    "QueryResult",
    "ValidationIssue",
    "ValidationResult",
    "register",
]

__version__ = "0.1.0"
