import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from datus_semantic_metricflow import MetricFlowAdapter, MetricFlowConfig
from datus_semantic_metricflow.models import (
    MetricDefinition,
    QueryResult,
    TimeGranularity,
    TimeRange,
    ValidationResult,
)


@pytest.fixture
def config():
    return MetricFlowConfig(
        namespace="test",
        project_root="/tmp/test_project",
        cli_path="mf",
    )


@pytest.fixture
def adapter(config):
    return MetricFlowAdapter(config)


class TestMetricFlowAdapter:
    @pytest.mark.asyncio
    async def test_list_metrics_json_format(self, adapter):
        mock_output = """[
            {
                "name": "revenue",
                "description": "Total revenue",
                "type": "simple",
                "dimensions": ["date", "region"]
            },
            {
                "name": "orders",
                "description": "Number of orders",
                "type": "simple",
                "dimensions": ["date"]
            }
        ]"""

        with patch.object(adapter, "_run_command", new_callable=AsyncMock) as mock_cmd:
            mock_cmd.return_value = (mock_output, "", 0)

            metrics = await adapter.list_metrics(limit=10)

            assert len(metrics) == 2
            assert metrics[0].name == "revenue"
            assert metrics[0].description == "Total revenue"
            assert "date" in metrics[0].dimensions
            assert metrics[1].name == "orders"

    @pytest.mark.asyncio
    async def test_list_metrics_with_pagination(self, adapter):
        mock_output = """[
            {"name": "metric1"},
            {"name": "metric2"},
            {"name": "metric3"},
            {"name": "metric4"},
            {"name": "metric5"}
        ]"""

        with patch.object(adapter, "_run_command", new_callable=AsyncMock) as mock_cmd:
            mock_cmd.return_value = (mock_output, "", 0)

            metrics = await adapter.list_metrics(limit=2, offset=1)

            assert len(metrics) == 2
            assert metrics[0].name == "metric2"
            assert metrics[1].name == "metric3"

    @pytest.mark.asyncio
    async def test_get_dimensions(self, adapter):
        mock_output = """["date", "region", "product"]"""

        with patch.object(adapter, "_run_command", new_callable=AsyncMock) as mock_cmd:
            mock_cmd.return_value = (mock_output, "", 0)

            dimensions = await adapter.get_dimensions("revenue")

            assert len(dimensions) == 3
            assert "date" in dimensions
            assert "region" in dimensions
            mock_cmd.assert_called_once_with(["list-dimensions", "--metrics", "revenue"])

    @pytest.mark.asyncio
    async def test_query_metrics_basic(self, adapter):
        mock_output = """{
            "columns": ["date", "revenue"],
            "data": [
                ["2024-01-01", 1000],
                ["2024-01-02", 1200]
            ]
        }"""

        with patch.object(adapter, "_run_command", new_callable=AsyncMock) as mock_cmd:
            mock_cmd.return_value = (mock_output, "", 0)

            result = await adapter.query_metrics(
                metrics=["revenue"],
                dimensions=["date"],
                limit=10,
            )

            assert result.columns == ["date", "revenue"]
            assert len(result.data) == 2
            assert result.data[0] == ["2024-01-01", 1000]

    @pytest.mark.asyncio
    async def test_query_metrics_with_time_range(self, adapter):
        with patch.object(adapter, "_run_command", new_callable=AsyncMock) as mock_cmd:
            mock_cmd.return_value = ('{"columns": [], "data": []}', "", 0)

            time_range = TimeRange(
                start="2024-01-01",
                end="2024-12-31",
                granularity=TimeGranularity.MONTH,
            )

            await adapter.query_metrics(
                metrics=["revenue"],
                dimensions=["date"],
                time_range=time_range,
            )

            call_args = mock_cmd.call_args[0][0]
            assert "--start-time" in call_args
            assert "2024-01-01" in call_args
            assert "--end-time" in call_args
            assert "2024-12-31" in call_args
            assert "--time-granularity" in call_args
            assert "month" in call_args

    @pytest.mark.asyncio
    async def test_query_metrics_dry_run(self, adapter):
        mock_sql = "SELECT date, SUM(revenue) FROM ... GROUP BY date"

        with patch.object(adapter, "_run_command", new_callable=AsyncMock) as mock_cmd:
            mock_cmd.return_value = (mock_sql, "", 0)

            result = await adapter.query_metrics(
                metrics=["revenue"],
                dimensions=["date"],
                dry_run=True,
            )

            assert result.columns == ["sql"]
            assert result.data[0][0] == mock_sql
            assert result.metadata.get("explain") is True
            call_args = mock_cmd.call_args[0][0]
            assert "--explain" in call_args

    @pytest.mark.asyncio
    async def test_validate_semantic_success(self, adapter):
        with patch.object(adapter, "_run_command", new_callable=AsyncMock) as mock_cmd:
            mock_cmd.return_value = ("Validation passed", "", 0)

            result = await adapter.validate_semantic()

            assert result.valid is True
            assert len(result.issues) == 0

    @pytest.mark.asyncio
    async def test_validate_semantic_with_errors(self, adapter):
        error_output = """Error: Invalid metric definition in revenue.yml
Warning: Deprecated dimension usage in orders"""

        with patch.object(adapter, "_run_command", new_callable=AsyncMock) as mock_cmd:
            mock_cmd.return_value = ("", error_output, 1)

            result = await adapter.validate_semantic()

            assert result.valid is False
            assert len(result.issues) == 2
            assert result.issues[0].severity == "error"
            assert result.issues[1].severity == "warning"

    @pytest.mark.asyncio
    async def test_command_timeout(self, adapter):
        with patch("asyncio.wait_for", side_effect=asyncio.TimeoutError):
            with pytest.raises(RuntimeError, match="timed out"):
                await adapter._run_command(["list-metrics"])

    @pytest.mark.asyncio
    async def test_cli_not_found(self, adapter):
        with patch(
            "asyncio.create_subprocess_exec",
            side_effect=FileNotFoundError("mf not found"),
        ):
            with pytest.raises(RuntimeError, match="CLI not found"):
                await adapter._run_command(["list-metrics"])

    def test_parse_metrics_table_format(self, adapter):
        table_output = """
Metric Name    Description
-----------    -----------
revenue        Total revenue
orders         Number of orders
customers      Active customers
"""

        metrics = adapter._parse_metrics_table(table_output)

        assert len(metrics) == 3
        assert metrics[0].name == "revenue"
        assert metrics[0].description == "Total revenue"
        assert metrics[1].name == "orders"

    def test_parse_dimensions_table_format(self, adapter):
        table_output = """
Dimension Name
--------------
date
region
product
"""

        dimensions = adapter._parse_dimensions_output(table_output)

        assert len(dimensions) == 3
        assert "date" in dimensions
        assert "region" in dimensions


class TestConfiguration:
    def test_config_defaults(self):
        config = MetricFlowConfig(namespace="test")

        assert config.namespace == "test"
        assert config.cli_path == "mf"
        assert config.project_root is None
        assert config.timeout == 300

    def test_config_custom_values(self):
        config = MetricFlowConfig(
            namespace="prod",
            cli_path="/usr/local/bin/mf",
            project_root="/var/projects/metrics",
            environment="production",
            timeout=600,
        )

        assert config.namespace == "prod"
        assert config.cli_path == "/usr/local/bin/mf"
        assert config.project_root == "/var/projects/metrics"
        assert config.environment == "production"
        assert config.timeout == 600
