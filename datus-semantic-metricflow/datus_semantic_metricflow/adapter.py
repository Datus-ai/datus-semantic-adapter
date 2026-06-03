import inspect
from pathlib import Path
from types import SimpleNamespace
from typing import List, Optional, Set, Tuple

import logging
import os

from datus_semantic_core import BaseSemanticAdapter
from datus_semantic_metricflow.config import MetricFlowConfig
from datus_semantic_metricflow.models import (
    DimensionInfo,
    MetricDefinition,
    QueryResult,
    ValidationIssue,
    ValidationResult,
)

# Import MetricFlow API
from metricflow.api.metricflow_client import MetricFlowClient
from metricflow.configuration.datus_config_handler import DatusConfigHandler
from metricflow.configuration.dict_config_handler import (
    DictConfigHandler,
    build_config_dict_from_db_params,
)
from metricflow.naming.linkable_spec_name import StructuredLinkableSpecName
from metricflow.references import MetricReference

try:
    from metricflow.configuration.dict_config_handler import build_config_dict_from_datus_datasource
except ImportError:
    build_config_dict_from_datus_datasource = None

logger = logging.getLogger(__name__)


class MetricFlowAdapter(BaseSemanticAdapter):
    """
    MetricFlow semantic layer adapter.

    Integrates with MetricFlow CLI to provide metric querying capabilities.
    """

    def __init__(self, config: MetricFlowConfig):
        super().__init__(config, service_type="metricflow")
        self.datasource = config.datasource
        self.timeout = config.timeout
        self._client_init_error: Optional[Exception] = None
        self._client_initialized = False

        logger.info(f"Initializing MetricFlowAdapter for datasource: {self.datasource}")

        try:
            # Import MetricFlow utilities
            from metricflow.configuration.constants import CONFIG_DWH_SCHEMA
            from metricflow.sql_clients.sql_utils import make_sql_client_from_config

            # Initialize config handler: dict-based or file-based
            if config.db_config:
                model_path = self._resolve_model_path(config)
                config_dict = self._build_metricflow_config_dict(config.db_config, model_path)
                self._config_handler = DictConfigHandler(config_dict)
                logger.info("Using DictConfigHandler (in-memory config, no file read)")
            else:
                config_path = getattr(config, "config_path", None)
                self._config_handler = DatusConfigHandler(
                    namespace=self.datasource, config_path=config_path
                )
                logger.info("Using DatusConfigHandler (reading agent.yml from disk)")

            # Build client components using the config handler
            sql_client = make_sql_client_from_config(self._config_handler)
            schema = self._config_handler.get_value(CONFIG_DWH_SCHEMA)
            self.client = SimpleNamespace(sql_client=sql_client, system_schema=schema)
            logger.info(
                "MetricFlowAdapter initialized; MetricFlowClient will load semantic YAML on first use"
            )

        except Exception as e:
            logger.error(f"Failed to initialize MetricFlowAdapter: {e}", exc_info=True)
            raise

    def _ensure_client_ready(self) -> MetricFlowClient:
        """Build the MetricFlowClient lazily so bad YAML does not break adapter startup."""
        if getattr(self, "_client_initialized", True):
            return self.client

        try:
            user_configured_model = self._build_user_configured_model_from_config(
                self._config_handler
            )
            self.client = MetricFlowClient(
                sql_client=self.client.sql_client,
                user_configured_model=user_configured_model,
                system_schema=self.client.system_schema,
            )
            self._client_initialized = True
            self._client_init_error = None
            logger.info("MetricFlowClient initialized successfully")
            return self.client
        except Exception as e:
            self._client_init_error = e
            logger.warning(
                "MetricFlowClient initialization deferred until semantic configuration is valid: %s",
                e,
            )
            logger.debug("Deferred MetricFlowClient initialization details", exc_info=True)
            raise RuntimeError(
                "MetricFlow semantic configuration is invalid. "
                "Run validate_semantic to inspect YAML errors before listing or querying metrics."
            ) from e

    @staticmethod
    def _resolve_model_path(config: MetricFlowConfig) -> str:
        """Resolve semantic models path from config."""

        if config.semantic_models_path:
            path = Path(config.semantic_models_path)
            path.mkdir(parents=True, exist_ok=True)
            return str(path)

        agent_home = config.agent_home or "~/.datus"
        path = Path(agent_home).expanduser().resolve() / "semantic_models" / config.datasource
        path.mkdir(parents=True, exist_ok=True)
        return str(path)

    @staticmethod
    def _build_metricflow_config_dict(db_config: dict, model_path: str) -> dict:
        if build_config_dict_from_datus_datasource is not None:
            return build_config_dict_from_datus_datasource(db_config, model_path=model_path)

        return MetricFlowAdapter._build_metricflow_config_dict_legacy(db_config, model_path)

    @staticmethod
    def _build_metricflow_config_dict_legacy(db_config: dict, model_path: str) -> dict:
        kwargs = {
            "db_type": db_config.get("type", ""),
            "host": db_config.get("host", ""),
            "port": str(db_config.get("port", "")),
            "username": db_config.get("username", ""),
            "password": db_config.get("password", ""),
            "database": db_config.get("database", ""),
            "schema": db_config.get("schema", ""),
            "uri": db_config.get("uri", ""),
            "warehouse": db_config.get("warehouse", ""),
            "account": db_config.get("account", ""),
            "project_id": db_config.get("project_id", ""),
            "model_path": model_path,
        }
        sslmode = db_config.get("sslmode")
        if sslmode and MetricFlowAdapter._build_config_supports_kwarg("sslmode"):
            kwargs["sslmode"] = sslmode
        unsupported_keys = []
        for optional_key in ("role", "private_key", "private_key_file", "private_key_file_pwd"):
            value = db_config.get(optional_key)
            if value is None or value == "":
                continue
            if MetricFlowAdapter._build_config_supports_kwarg(optional_key):
                kwargs[optional_key] = str(value)
            else:
                unsupported_keys.append(optional_key)
        if unsupported_keys:
            unsupported = ", ".join(unsupported_keys)
            raise RuntimeError(
                "Installed datus-metricflow does not support Snowflake config fields: "
                f"{unsupported}. Install the matching datus-metricflow build before using Snowflake metrics."
            )
        return build_config_dict_from_db_params(**kwargs)

    @staticmethod
    def _build_config_supports_kwarg(name: str) -> bool:
        signature = inspect.signature(build_config_dict_from_db_params)
        return name in signature.parameters or any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )

    @staticmethod
    def _collect_model_file_paths(model_path: str) -> List[str]:
        """Collect MetricFlow YAML files from the configured model path.

        MetricFlow's directory collector skips files ignored by the nearest
        .gitignore. Datus stores generated semantic models under project-local
        subject/ directories, which are intentionally gitignored, so the adapter
        must treat the configured semantic_models_path as authoritative.
        """
        config_file_paths: List[str] = []
        for root, dirs, files in os.walk(model_path):
            dirs[:] = [directory for directory in dirs if not directory.startswith(".")]
            for file_name in files:
                if file_name.startswith("."):
                    continue
                if Path(file_name).suffix.lower() not in {".yml", ".yaml"}:
                    continue
                config_file_paths.append(os.path.join(root, file_name))
        return sorted(config_file_paths)

    @classmethod
    def _model_build_result_from_config(cls, handler, raise_issues_as_exceptions: bool = True):
        from metricflow.engine.utils import path_to_models
        from metricflow.model.parsing.dir_to_model import parse_yaml_file_paths_to_model

        model_path = path_to_models(handler=handler)
        file_paths = cls._collect_model_file_paths(model_path)
        return parse_yaml_file_paths_to_model(
            file_paths,
            raise_issues_as_exceptions=raise_issues_as_exceptions,
        )

    @classmethod
    def _build_user_configured_model_from_config(cls, handler):
        return cls._model_build_result_from_config(handler).model

    # Semantic Model Interface

    def get_semantic_model(
        self,
        table_name: str,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
    ):
        """MetricFlow doesn't directly expose semantic models."""
        return None

    def list_semantic_models(
        self,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
    ):
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
        List available metrics using MetricFlow client.

        Args:
            path: Optional subject area filter
            limit: Maximum metrics to return
            offset: Number to skip

        Returns:
            List of metric definitions
        """
        client = self._ensure_client_ready()

        # Get full metric objects directly from semantic_model
        metric_semantics = client.semantic_model.metric_semantics
        metric_refs = metric_semantics.metric_references
        full_metrics = metric_semantics.get_metrics(metric_refs)

        # Convert to MetricDefinition list
        metrics = []
        for metric in full_metrics:
            # Get dimensions for this metric
            dimensions = client.engine.simple_dimensions_for_metrics([metric.name])
            metrics.append(
                MetricDefinition(
                    name=metric.name,
                    description=metric.description,
                    type=metric.type,
                    dimensions=[d.name for d in dimensions],
                    measures=[m.name for m in metric.input_measures],
                    metadata={},
                )
            )

        if path:
            metrics = [m for m in metrics if m.path and m.path[: len(path)] == path]

        return metrics[offset : offset + limit]

    async def get_dimensions(
        self,
        metric_name: str,
        path: Optional[List[str]] = None,
    ) -> List[DimensionInfo]:
        """
        Get dimensions for a metric using MetricFlow client.

        Args:
            metric_name: Name of the metric
            path: Optional subject area filter

        Returns:
            List of DimensionInfo objects containing name and description
        """
        client = self._ensure_client_ready()

        # Get dimensions from client (returns List[Dimension])
        dimensions = client.list_dimensions(metric_names=[metric_name])

        # Convert to DimensionInfo objects
        return [DimensionInfo(name=d.name, description=d.description) for d in dimensions]

    @staticmethod
    def _normalize_time_granularity(granularity: Optional[str]) -> Optional[str]:
        if granularity is None:
            return None
        value = str(granularity).strip().lower()
        return value or None

    @staticmethod
    def _time_dimension_names_for_metrics(client, metrics: List[str]) -> Set[str]:
        """Return queryable time dimension names for the metric set.

        MetricFlow's public ``Dimension`` dataclass only exposes name and
        description. For canonicalization we inspect the internal linkable specs
        and use their type via the presence of ``time_granularity``.
        """
        try:
            metric_refs = [MetricReference(element_name=metric) for metric in metrics]
            specs = client.semantic_model.metric_semantics.element_specs_for_metrics(metric_refs)
        except Exception:
            return set()

        names: Set[str] = set()
        for spec in specs:
            if getattr(spec, "time_granularity", None) is None:
                continue

            element_name = getattr(spec, "element_name", None)
            if isinstance(element_name, str) and element_name:
                names.add(element_name.lower())

            qualified_name = getattr(spec, "qualified_name", None)
            if isinstance(qualified_name, str) and qualified_name:
                parsed = StructuredLinkableSpecName.from_name(qualified_name.lower())
                names.add(parsed.qualified_name_without_granularity)
                names.add(parsed.element_name)

            identifier_links = getattr(spec, "identifier_links", ()) or ()
            identifier_names = []
            for identifier in identifier_links:
                identifier_name = getattr(identifier, "element_name", None)
                if isinstance(identifier_name, str) and identifier_name:
                    identifier_names.append(identifier_name.lower())
            if identifier_names and isinstance(element_name, str) and element_name:
                names.add("__".join(identifier_names + [element_name.lower()]))

        return names

    @staticmethod
    def _is_known_time_dimension_name(name: str, time_dimension_names: Set[str]) -> bool:
        if not time_dimension_names:
            return False

        parsed = StructuredLinkableSpecName.from_name(name.lower())
        return (
            parsed.element_name in time_dimension_names
            or parsed.qualified_name_without_granularity in time_dimension_names
            or parsed.qualified_name in time_dimension_names
        )

    @classmethod
    def _canonicalize_time_query_params(
        cls,
        client,
        metrics: List[str],
        dimensions: Optional[List[str]],
        order_by: Optional[List[str]],
        granularity: Optional[str],
    ) -> Tuple[List[str], Optional[List[str]]]:
        query_dimensions = list(dimensions) if dimensions else []
        order_list = [o for o in (order_by or []) if o and o != "null"] or None

        normalized_granularity = cls._normalize_time_granularity(granularity)
        if not normalized_granularity:
            return query_dimensions, order_list

        metric_time_dimension = f"metric_time__{normalized_granularity}"
        time_dimension_names = cls._time_dimension_names_for_metrics(client, metrics)
        canonical_dimensions = []
        for dimension in query_dimensions:
            parsed = StructuredLinkableSpecName.from_name(dimension.lower())
            is_metric_time = parsed.element_name == "metric_time"
            is_time_dimension = cls._is_known_time_dimension_name(dimension, time_dimension_names)
            if is_metric_time or is_time_dimension:
                continue
            canonical_dimensions.append(dimension)

        if metric_time_dimension not in canonical_dimensions:
            canonical_dimensions.append(metric_time_dimension)

        if not order_list:
            return canonical_dimensions, None

        canonical_order = []
        for order_item in order_list:
            descending = order_item.startswith("-")
            order_name = order_item[1:] if descending else order_item
            parsed = StructuredLinkableSpecName.from_name(order_name.lower())
            is_metric_time = parsed.element_name == "metric_time"
            is_time_dimension = cls._is_known_time_dimension_name(order_name, time_dimension_names)
            if is_metric_time or is_time_dimension:
                order_name = metric_time_dimension

            canonical_item = f"-{order_name}" if descending else order_name
            if canonical_item not in canonical_order:
                canonical_order.append(canonical_item)

        return canonical_dimensions, canonical_order

    async def query_metrics(
        self,
        metrics: List[str],
        dimensions: List[str] = [],
        path: Optional[List[str]] = None,
        time_start: Optional[str] = None,
        time_end: Optional[str] = None,
        time_granularity: Optional[str] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
        order_by: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> QueryResult:
        """
        Query metrics using MetricFlow client.

        Args:
            metrics: List of metric names
            dimensions: List of dimensions to group by
            path: Optional subject area filter
            time_start: Start time (ISO format like '2024-01-01' or relative like '-7d')
            time_end: End time (ISO format like '2024-01-31' or relative like 'now')
            time_granularity: Time granularity for aggregation ('day', 'week', 'month', 'quarter', 'year')
            where: Optional WHERE clause
            limit: Result limit
            order_by: Columns to order by
            dry_run: If True, explain query instead of executing

        Returns:
            Query result
        """
        client = self._ensure_client_ready()

        # Helper to convert "null" string to None
        def _normalize_null(value):
            if value is None or value == "null" or value == "":
                return None
            return value

        # Prepare query parameters (normalize "null" strings to None)
        start_time = _normalize_null(time_start)
        end_time = _normalize_null(time_end)
        granularity = self._normalize_time_granularity(_normalize_null(time_granularity))
        where_clause = _normalize_null(where)

        query_dimensions, order_list = self._canonicalize_time_query_params(
            client=client,
            metrics=metrics,
            dimensions=dimensions,
            order_by=order_by,
            granularity=granularity,
        )

        if dry_run:
            # Use explain to get SQL without executing
            result = client.explain(
                metrics=metrics,
                dimensions=query_dimensions,
                start_time=start_time,
                end_time=end_time,
                where=where_clause,
                limit=limit,
                order=order_list,
            )
            # Return SQL as result
            sql = result.rendered_sql_without_descriptions.sql_query
            return QueryResult(
                columns=["sql"],
                data=[{"sql": sql}],
                metadata={"explain": True, "sql": sql},
            )
        else:
            # Execute the query
            logger.debug(
                f"Executing query: metrics={metrics}, dimensions={query_dimensions}, "
                f"start_time={start_time}, end_time={end_time}, where={where_clause}, limit={limit}"
            )
            result = client.query(
                metrics=metrics,
                dimensions=query_dimensions,
                start_time=start_time,
                end_time=end_time,
                where=where_clause,
                limit=limit,
                order=order_list,
            )
            logger.debug(
                f"Query result: result_df={result.result_df is not None}, empty={result.result_df.empty if result.result_df is not None else 'N/A'}"
            )

            # Convert DataFrame to QueryResult
            if result.result_df is not None and not result.result_df.empty:
                columns = result.result_df.columns.tolist()
                # Convert to list of dicts (QueryResult.data expects List[Dict[str, Any]])
                data = result.result_df.to_dict(orient="records")
                return QueryResult(
                    columns=columns,
                    data=data,
                    metadata={"dataflow_plan": result.dataflow_plan},
                )
            else:
                return QueryResult(columns=[], data=[])

    async def validate_semantic(
        self,
        scope: str = "all",
        validation_scope: Optional[str] = None,
    ) -> ValidationResult:
        """
        Validate MetricFlow configuration using full validation pipeline.

        This performs the same validations as 'mf validate-configs':
        1. Lint validation (YAML format)
        2. Parsing validation (model building)
        3. Semantic validation (model semantics)
        4. Data warehouse validation

        Args:
            scope: "all" validates semantic models and metrics. "semantic_model"
                validates semantic models before metric files exist: the expected
                no-metrics semantic issue is ignored, but data-source, dimension,
                identifier, and measure warehouse validations still run.
            validation_scope: Alias for scope.

        Returns:
            Validation result
        """
        from metricflow.engine.utils import path_to_models
        from metricflow.model.model_validator import ModelValidator
        from metricflow.model.parsing.config_linter import ConfigLinter
        from metricflow.model.data_warehouse_model_validator import DataWarehouseModelValidator

        scope = validation_scope if validation_scope is not None else scope
        if scope is None:
            scope = "all"
        if scope not in {"all", "semantic_model"}:
            return ValidationResult(
                valid=False,
                issues=[
                    ValidationIssue(
                        severity="error",
                        message="scope must be one of: all, semantic_model",
                    )
                ],
            )

        all_issues: List[ValidationIssue] = []

        # Step 1: Lint Validation
        try:
            model_path = path_to_models(handler=self._config_handler)
            config_file_paths = self._collect_model_file_paths(model_path)
            lint_results = ConfigLinter().lint_files(config_file_paths)
            all_issues.extend(self._convert_validation_results(lint_results))
            if lint_results.has_blocking_issues:
                return ValidationResult(valid=False, issues=all_issues)
        except Exception as e:
            logger.error(f"Lint validation failed: {e}")
            all_issues.append(
                ValidationIssue(severity="error", message=f"Lint validation failed: {e}")
            )
            return ValidationResult(valid=False, issues=all_issues)

        # Step 2: Parsing Validation
        try:
            parsing_result = self._model_build_result_from_config(
                self._config_handler,
                raise_issues_as_exceptions=False,
            )
            all_issues.extend(self._convert_validation_results(parsing_result.issues))
            if parsing_result.issues.has_blocking_issues:
                return ValidationResult(valid=False, issues=all_issues)
            user_model = parsing_result.model
        except Exception as e:
            logger.error(f"Parsing validation failed: {e}")
            all_issues.append(
                ValidationIssue(severity="error", message=f"Parsing validation failed: {e}")
            )
            return ValidationResult(valid=False, issues=all_issues)

        # Step 3: Semantic Validation
        try:
            semantic_result = ModelValidator().validate_model(user_model)
            semantic_issues = self._convert_validation_results(semantic_result.issues)
            if scope == "semantic_model":
                effective_semantic_issues = [
                    issue
                    for issue in semantic_issues
                    if not self._is_no_metrics_present_issue(issue)
                ]
            else:
                effective_semantic_issues = semantic_issues
            all_issues.extend(effective_semantic_issues)
            if semantic_result.issues.has_blocking_issues:
                has_effective_errors = any(
                    issue.severity == "error" for issue in effective_semantic_issues
                )
                if has_effective_errors or scope != "semantic_model":
                    return ValidationResult(valid=False, issues=all_issues)
        except Exception as e:
            logger.error(f"Semantic validation failed: {e}")
            all_issues.append(
                ValidationIssue(severity="error", message=f"Semantic validation failed: {e}")
            )
            return ValidationResult(valid=False, issues=all_issues)

        # Step 4: Data Warehouse Validation
        try:
            dw_validator = DataWarehouseModelValidator(
                sql_client=self.client.sql_client,
                system_schema=self.client.system_schema,
            )
            dw_results = self._run_dw_validations(
                dw_validator,
                user_model,
                include_metrics=scope == "all",
            )
            all_issues.extend(self._convert_validation_results(dw_results))
        except Exception as e:
            logger.error(f"Data warehouse validation failed: {e}")
            all_issues.append(
                ValidationIssue(severity="error", message=f"Data warehouse validation failed: {e}")
            )

        has_errors = any(issue.severity == "error" for issue in all_issues)
        return ValidationResult(valid=not has_errors, issues=all_issues)

    def _run_dw_validations(self, dw_validator, model, *, include_metrics: bool = True):
        """Run all data warehouse validations and merge results."""
        from metricflow.model.validations.validator_helpers import ModelValidationResults

        timeout = self.timeout
        results = [
            dw_validator.validate_data_sources(model, timeout),
            dw_validator.validate_dimensions(model, timeout),
            dw_validator.validate_identifiers(model, timeout),
            dw_validator.validate_measures(model, timeout),
        ]
        if include_metrics:
            results.append(dw_validator.validate_metrics(model, timeout))

        return ModelValidationResults.merge(results)

    def _convert_validation_results(self, results) -> List[ValidationIssue]:
        """Convert ModelValidationResults to list of ValidationIssue."""
        issues = []
        for error in results.errors:
            issues.append(
                ValidationIssue(
                    severity="error",
                    message=str(error),
                )
            )
        for warning in results.warnings:
            issues.append(
                ValidationIssue(
                    severity="warning",
                    message=str(warning),
                )
            )
        return issues

    @staticmethod
    def _is_no_metrics_present_issue(issue: ValidationIssue) -> bool:
        return issue.severity == "error" and "No metrics present in the model" in str(issue.message)
