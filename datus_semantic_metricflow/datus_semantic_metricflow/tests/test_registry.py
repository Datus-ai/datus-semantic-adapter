import pytest

from datus_semantic_metricflow import (
    MetricFlowAdapter,
    MetricFlowConfig,
    semantic_adapter_registry,
)


class TestSemanticAdapterRegistry:
    def test_adapter_auto_registered(self):
        adapters = semantic_adapter_registry.list_adapters()

        assert "metricflow" in adapters
        assert adapters["metricflow"] == "MetricFlow"

    def test_create_adapter(self):
        config = MetricFlowConfig(
            namespace="test",
            project_root="/tmp/test",
        )

        adapter = semantic_adapter_registry.create_adapter("metricflow", config)

        assert isinstance(adapter, MetricFlowAdapter)
        assert adapter.service_type == "metricflow"
        assert adapter.project_root == "/tmp/test"

    def test_create_adapter_unknown_type(self):
        config = MetricFlowConfig(namespace="test")

        with pytest.raises(ValueError, match="not found"):
            semantic_adapter_registry.create_adapter("unknown_adapter", config)

    def test_get_config_class(self):
        config_class = semantic_adapter_registry.get_config_class("metricflow")

        assert config_class == MetricFlowConfig

    def test_get_config_class_unknown(self):
        config_class = semantic_adapter_registry.get_config_class("unknown")

        assert config_class is None
