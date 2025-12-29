import asyncio
import json
import re
from typing import Any, Dict, List, Optional

from datus.tools.semantic_tools.base import BaseSemanticAdapter
from datus_semantic_metricflow.config import MetricFlowConfig
from datus_semantic_metricflow.models import (
    MetricDefinition,
    MetricType,
    QueryResult,
    TimeRange,
    ValidationIssue,
    ValidationResult,
)


class MetricFlowAdapter(BaseSemanticAdapter):
    """
    MetricFlow semantic layer adapter.

    Integrates with MetricFlow CLI to provide metric querying capabilities.
    """

    def __init__(self, config: MetricFlowConfig):
        super().__init__(config, service_type="metricflow")
        self.cli_path = config.cli_path
        self.namespace = config.namespace
        self.project_root = config.project_root
        self.environment = config.environment
        self.timeout = config.timeout

    async def _run_command(self, args: List[str]) -> tuple[str, str, int]:
        """
        Run MetricFlow CLI command asynchronously.

        Args:
            args: Command arguments (excluding cli_path)

        Returns:
            Tuple of (stdout, stderr, returncode)
        """
        cmd = [self.cli_path]

        if self.namespace:
            cmd.extend(["--namespace", self.namespace])

        cmd.extend(args)

        if self.project_root:
            cmd.extend(["--project-root", self.project_root])

        if self.environment:
            cmd.extend(["--environment", self.environment])

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=self.timeout,
            )

            return (
                stdout.decode("utf-8"),
                stderr.decode("utf-8"),
                proc.returncode or 0,
            )
        except asyncio.TimeoutError:
            raise RuntimeError(f"MetricFlow command timed out after {self.timeout}s")
        except FileNotFoundError:
            raise RuntimeError(f"MetricFlow CLI not found at: {self.cli_path}")

    # Semantic Model Interface

    def get_semantic_model(
        self,
        table_name: str,
        catalog: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """MetricFlow doesn't directly expose semantic models."""
        return None

    def list_semantic_models(
        self,
        catalog: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> List[str]:
        """MetricFlow uses semantic models internally."""
        return []

    # Metrics Interface

    async def list_metrics(
        self,
        path: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[MetricDefinition]:
        """
        List available metrics using 'mf list-metrics'.

        Args:
            path: Optional subject area filter
            limit: Maximum metrics to return
            offset: Number to skip

        Returns:
            List of metric definitions
        """
        stdout, stderr, returncode = await self._run_command(["list-metrics"])

        if returncode != 0:
            raise RuntimeError(f"Failed to list metrics: {stderr}")

        metrics = self._parse_metrics_output(stdout)

        if path:
            metrics = [m for m in metrics if m.path and m.path[: len(path)] == path]

        return metrics[offset : offset + limit]

    async def get_dimensions(
        self,
        metric_name: str,
        path: Optional[List[str]] = None,
    ) -> List[str]:
        """
        Get dimensions for a metric using 'mf list-dimensions'.

        Args:
            metric_name: Name of the metric
            path: Optional subject area filter

        Returns:
            List of dimension names
        """
        stdout, stderr, returncode = await self._run_command(
            ["list-dimensions", "--metrics", metric_name]
        )

        if returncode != 0:
            raise RuntimeError(f"Failed to get dimensions for {metric_name}: {stderr}")

        return self._parse_dimensions_output(stdout)

    async def query_metrics(
        self,
        metrics: List[str],
        dimensions: List[str] = [],
        path: Optional[List[str]] = None,
        time_range: Optional[TimeRange] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        order_by: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> QueryResult:
        """
        Query metrics using 'mf query'.

        Args:
            metrics: List of metric names
            dimensions: List of dimensions to group by
            path: Optional subject area filter
            time_range: Optional time range
            where: Optional WHERE clause
            limit: Result limit
            order_by: Columns to order by
            dry_run: If True, explain query instead of executing

        Returns:
            Query result
        """
        args = ["query", "--metrics", ",".join(metrics)]

        if dimensions:
            args.extend(["--group-by", ",".join(dimensions)])

        if time_range:
            if time_range.start:
                args.extend(["--start-time", time_range.start])
            if time_range.end:
                args.extend(["--end-time", time_range.end])
            if time_range.granularity:
                args.extend(["--time-granularity", time_range.granularity.value])

        if where:
            args.extend(["--where", where])

        if limit:
            args.extend(["--limit", str(limit)])

        if order_by:
            args.extend(["--order", ",".join(order_by)])

        if dry_run:
            args.append("--explain")

        stdout, stderr, returncode = await self._run_command(args)

        if returncode != 0:
            raise RuntimeError(f"Query failed: {stderr}")

        return self._parse_query_result(stdout, dry_run)

    async def validate_semantic(self) -> ValidationResult:
        """
        Validate MetricFlow configuration using 'mf validate-configs'.

        Returns:
            Validation result
        """
        stdout, stderr, returncode = await self._run_command(["validate-configs"])

        valid = returncode == 0
        issues = []

        if not valid:
            issues = self._parse_validation_errors(stderr)

        return ValidationResult(valid=valid, issues=issues)

    # Parsing helpers

    def _parse_metrics_output(self, output: str) -> List[MetricDefinition]:
        """
        Parse 'mf list-metrics' output.

        Expected format: JSON array or table format.
        """
        metrics = []

        try:
            data = json.loads(output)
            if isinstance(data, list):
                for item in data:
                    metrics.append(self._metric_from_dict(item))
            elif isinstance(data, dict) and "metrics" in data:
                for item in data["metrics"]:
                    metrics.append(self._metric_from_dict(item))
        except json.JSONDecodeError:
            metrics = self._parse_metrics_table(output)

        return metrics

    def _metric_from_dict(self, data: Dict[str, Any]) -> MetricDefinition:
        """Convert metric dictionary to MetricDefinition."""
        return MetricDefinition(
            name=data.get("name", ""),
            description=data.get("description"),
            type=self._map_metric_type(data.get("type")),
            dimensions=data.get("dimensions", []),
            measures=data.get("measures", []),
            metadata=data,
        )

    def _map_metric_type(self, type_str: Optional[str]) -> Optional[MetricType]:
        """Map MetricFlow metric type to MetricType enum."""
        if not type_str:
            return None

        type_map = {
            "simple": MetricType.SIMPLE,
            "ratio": MetricType.RATIO,
            "cumulative": MetricType.CUMULATIVE,
            "derived": MetricType.DERIVED,
        }
        return type_map.get(type_str.lower())

    def _parse_metrics_table(self, output: str) -> List[MetricDefinition]:
        """Parse table-format output from list-metrics."""
        metrics = []
        lines = output.strip().split("\n")

        for line in lines[2:]:  # Skip header rows
            line = line.strip()
            if not line or line.startswith("-"):
                continue

            parts = re.split(r"\s{2,}", line)
            if len(parts) >= 1:
                metrics.append(
                    MetricDefinition(
                        name=parts[0].strip(),
                        description=parts[1].strip() if len(parts) > 1 else None,
                    )
                )

        return metrics

    def _parse_dimensions_output(self, output: str) -> List[str]:
        """Parse 'mf list-dimensions' output."""
        dimensions = []

        try:
            data = json.loads(output)
            if isinstance(data, list):
                dimensions = [str(d) for d in data]
            elif isinstance(data, dict) and "dimensions" in data:
                dimensions = [str(d) for d in data["dimensions"]]
        except json.JSONDecodeError:
            lines = output.strip().split("\n")
            for line in lines[2:]:  # Skip headers
                line = line.strip()
                if line and not line.startswith("-"):
                    parts = re.split(r"\s{2,}", line)
                    if parts:
                        dimensions.append(parts[0].strip())

        return dimensions

    def _parse_query_result(self, output: str, dry_run: bool) -> QueryResult:
        """Parse query result or explain plan."""
        if dry_run:
            return QueryResult(
                columns=["sql"],
                data=[[output]],
                metadata={"explain": True, "sql": output},
            )

        try:
            data = json.loads(output)
            if isinstance(data, dict) and "columns" in data and "data" in data:
                return QueryResult(
                    columns=data["columns"],
                    data=data["data"],
                    metadata=data.get("metadata", {}),
                )
        except json.JSONDecodeError:
            pass

        lines = output.strip().split("\n")
        if len(lines) < 2:
            return QueryResult(columns=[], data=[])

        columns = re.split(r"\s{2,}", lines[0].strip())
        data = []

        for line in lines[2:]:  # Skip header and separator
            line = line.strip()
            if line and not line.startswith("-"):
                row = re.split(r"\s{2,}", line)
                data.append(row)

        return QueryResult(columns=columns, data=data)

    def _parse_validation_errors(self, error_output: str) -> List[ValidationIssue]:
        """Parse validation error messages."""
        issues = []
        lines = error_output.strip().split("\n")

        for line in lines:
            line = line.strip()
            if not line:
                continue

            severity = "error"
            if "warning" in line.lower():
                severity = "warning"
            elif "info" in line.lower():
                severity = "info"

            issues.append(
                ValidationIssue(
                    severity=severity,
                    message=line,
                )
            )

        return issues
