# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""End-to-end adapter tests against the real engine and a seeded DuckDB file."""

from __future__ import annotations

import shutil

import pytest
from datus_semantic_dosi.adapter import DosiAdapter
from datus_semantic_dosi.config import DosiConfig
from datus_semantic_dosi.errors import SemanticValidationException


def _real_binding_available() -> bool:
    try:
        import dosi_engine
    except ImportError:
        return False
    # The unit-test fake sets this marker; the real extension does not.
    return not getattr(dosi_engine, "__dosi_fake__", False)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _real_binding_available(),
        reason="real dosi-engine bindings not installed",
    ),
    pytest.mark.skipif(
        shutil.which("duckdb") is None, reason="duckdb CLI not installed"
    ),
]


def _adapter(model_path: str, seeded_db: str | None = None) -> DosiAdapter:
    kwargs = {"semantic_model_path": model_path}
    if seeded_db is not None:
        kwargs["db_config"] = {"type": "duckdb", "uri": seeded_db}
    return DosiAdapter(DosiConfig(**kwargs))


async def test_list_metrics_and_dimensions(model_path):
    adapter = _adapter(model_path)
    names = {m.name for m in await adapter.list_metrics()}
    assert {"revenue", "order_count", "unique_customers"} <= names

    dims = {d.name: d for d in await adapter.get_dimensions("revenue")}
    assert "orders.order_date" in dims and dims["orders.order_date"].type == "time"
    assert "customers.region" in dims


async def test_dry_run_emits_sql(model_path):
    adapter = _adapter(model_path)
    result = await adapter.query_metrics(
        metrics=["revenue"], dimensions=["orders.status"], dry_run=True
    )
    assert result.columns == ["sql"]
    sql = result.data[0]["sql"]
    assert "main.orders" in sql
    # Compiled with pretty=True, so the engine formats it over several lines.
    assert "\n" in sql
    assert result.metadata["dry_run"] is True


async def test_execute_returns_rows(model_path, seeded_db):
    adapter = _adapter(model_path, seeded_db)
    result = await adapter.query_metrics(
        metrics=["revenue"],
        dimensions=["orders.status"],
        order_by=["-revenue"],
    )
    assert result.metadata["row_count"] > 0
    assert result.columns == ["status", "revenue"]
    by_status = {r["status"]: r["revenue"] for r in result.data}
    # Oracle from the seed: completed=350, cancelled=100.
    assert by_status["completed"] == 350
    assert by_status["cancelled"] == 100


async def test_execute_with_time_grain(model_path, seeded_db):
    adapter = _adapter(model_path, seeded_db)
    result = await adapter.query_metrics(
        metrics=["revenue"],
        dimensions=["orders.order_date"],
        time_granularity="month",
    )
    assert "order_date__month" in result.columns
    assert result.metadata["row_count"] > 0


async def test_native_structured_window_uses_metric_time_and_exposes_metadata(
    model_path, seeded_db
):
    adapter = _adapter(model_path, seeded_db)
    metrics = {metric.name: metric for metric in await adapter.list_metrics()}
    assert metrics["running_revenue"].type == "window"
    assert metrics["rolling_2m_avg_revenue"].metadata["window"] == {
        "type": "rolling",
        "function": "avg",
        "periods": 2,
    }

    dimensions = {
        dimension.name: dimension
        for dimension in await adapter.get_dimensions("running_revenue")
    }
    assert dimensions["orders.order_date"].time_granularities == [
        "day",
        "week",
        "month",
        "quarter",
        "year",
    ]

    result = await adapter.query_metrics(
        metrics=[
            "running_revenue",
            "rolling_2m_avg_revenue",
            "revenue_mom_growth",
        ],
        dimensions=["metric_time"],
        time_granularity="month",
        order_by=["metric_time__month"],
    )
    assert result.columns == [
        "metric_time__month",
        "running_revenue",
        "rolling_2m_avg_revenue",
        "revenue_mom_growth",
    ]
    assert [row["running_revenue"] for row in result.data] == [150, 380, 450]
    assert [row["rolling_2m_avg_revenue"] for row in result.data] == [
        150,
        190,
        150,
    ]
    assert result.data[0]["revenue_mom_growth"] is None
    assert result.data[1]["revenue_mom_growth"] == pytest.approx(80 / 150)


async def test_window_discovery_uses_native_axis_and_grain_validation(
    model_path, seeded_db
):
    adapter = _adapter(model_path, seeded_db)
    metrics = {metric.name: metric for metric in await adapter.list_metrics()}

    assert metrics["monthly_revenue_rank"].metadata["requires_time_axis"] is True

    dimensions = {
        dimension.name: dimension
        for dimension in await adapter.get_dimensions("monthly_running_revenue")
    }
    assert "orders.status" in dimensions
    assert dimensions["orders.order_date"].time_granularities == [
        "day",
        "week",
        "month",
    ]


async def test_bare_ambiguous_dimension_is_structured(model_path):
    adapter = _adapter(model_path)
    with pytest.raises(SemanticValidationException) as exc:
        # customer_id exists on both orders and customers. The adapter leaves
        # it bare so Dosi remains the single authority for name resolution.
        await adapter.query_metrics(
            metrics=["revenue"], dimensions=["customer_id"], dry_run=True
        )
    payload = exc.value.payload
    assert payload.code == "ambiguous_dimension"
    assert "customer_id" in payload.message


async def test_unknown_metric_is_structured(model_path):
    adapter = _adapter(model_path)
    with pytest.raises(SemanticValidationException) as exc:
        await adapter.query_metrics(metrics=["revenues"], dry_run=True)
    assert exc.value.payload.code == "unknown_metric"


async def test_validate_semantic_ok(model_path):
    adapter = _adapter(model_path)
    result = await adapter.validate_semantic(
        scope="semantic_model",
        semantic_model_name="orders_model",
    )
    assert result.valid is True
    assert result.issues == []


async def test_directory_routes_metrics_to_their_owning_model(model_path, tmp_path):
    shutil.copyfile(model_path, tmp_path / "orders.yaml")
    (tmp_path / "customer_catalog.yaml").write_text(
        """
version: "0.2.0.dev0"
semantic_model:
  - name: customer_catalog_model
    datasets:
      - name: customer_catalog
        source: main.customers
        primary_key: [customer_id]
        fields:
          - name: customer_id
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: customer_id
          - name: region
            expression:
              dialects:
                - dialect: ANSI_SQL
                  expression: region
    metrics:
      - name: customer_row_count
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: COUNT(customer_catalog.customer_id)
""".lstrip()
    )
    adapter = DosiAdapter(DosiConfig(semantic_models_path=str(tmp_path)))

    names = {metric.name for metric in await adapter.list_metrics()}
    assert {"revenue", "customer_row_count"} <= names

    result = await adapter.query_metrics(
        metrics=["customer_row_count"],
        dimensions=["customer_catalog.region"],
        dry_run=True,
    )
    assert "main.customers" in result.data[0]["sql"]

    with pytest.raises(SemanticValidationException) as exc:
        await adapter.query_metrics(
            metrics=["revenue", "customer_row_count"], dry_run=True
        )
    assert exc.value.payload.code == "cross_semantic_model_query_unsupported"


@pytest.fixture(scope="session")
def seeded_sqlite_db(tmp_path_factory) -> str:
    """Same orders oracle as ``seeded_db``, materialized as a SQLite file."""
    import sqlite3

    db = tmp_path_factory.mktemp("osi-sqlite") / "orders.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE orders (
            order_id INTEGER, customer_id INTEGER, product_id INTEGER,
            order_date DATE, status VARCHAR, amount DOUBLE
        );
        INSERT INTO orders VALUES
            (1, 1, 10, '2024-01-05', 'completed', 100),
            (2, 1, 11, '2024-01-20', 'completed', 50),
            (3, 2, 10, '2024-02-10', 'completed', 80),
            (4, 2, 11, '2024-02-15', 'cancelled', 30),
            (5, 3, 12, '2024-02-20', 'completed', 120),
            (6, 1, 12, '2024-03-01', 'cancelled', 70);
        CREATE TABLE customers (customer_id INTEGER, region VARCHAR, signup_date DATE);
        INSERT INTO customers VALUES
            (1, 'east', '2023-12-01'), (2, 'west', '2023-11-15'), (3, 'east', '2024-01-10');
        CREATE TABLE products (product_id INTEGER, category VARCHAR, unit_cost DOUBLE);
        INSERT INTO products VALUES (10, 'books', 20), (11, 'toys', 10), (12, 'books', 60);
        """
    )
    conn.commit()
    conn.close()
    return str(db)


async def test_execute_against_sqlite_datasource(model_path, seeded_sqlite_db):
    """A `type: sqlite` datasource executes through the DuckDB companion bridge.

    The engine has no SQLite dialect; the adapter rewrites the connection to a
    DuckDB companion of ``sqlite_scan`` views, so the same oracle numbers must
    come back as with the native DuckDB seed. Requires DuckDB to autoload its
    sqlite extension (network on first ever use, then cached).
    """
    adapter = DosiAdapter(
        DosiConfig(
            semantic_model_path=model_path,
            db_config={"type": "sqlite", "uri": seeded_sqlite_db},
            datasource="bird_like_sqlite",
        )
    )
    result = await adapter.query_metrics(
        metrics=["revenue"],
        dimensions=["orders.status"],
        order_by=["-revenue"],
    )
    assert result.metadata["row_count"] == 2
    by_status = {r["status"]: r["revenue"] for r in result.data}
    assert by_status == {"completed": 350.0, "cancelled": 100.0}

    dims = {d.name for d in await adapter.get_dimensions("revenue")}
    assert "customers.region" in dims
