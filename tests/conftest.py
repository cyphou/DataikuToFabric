"""Shared pytest fixtures for the Dataiku → Fabric Migration tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.agents.base_agent import AgentResult, AgentStatus
from src.core.config import (
    AppConfig,
    DataikuConfig,
    FabricConfig,
    LoggingConfig,
    MigrationConfig,
    OrchestratorConfig,
)
from src.core.registry import AssetRegistry
from src.models.asset import Asset, AssetType, MigrationState

PROJECT = "TEST_PROJECT"
FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_json(relative_path: str) -> dict | list:
    """Load a JSON fixture file relative to the fixtures directory."""
    return json.loads((FIXTURES_DIR / relative_path).read_text(encoding="utf-8"))


@pytest.fixture
def app_config(tmp_path) -> AppConfig:
    """Minimal AppConfig for testing."""
    return AppConfig(
        dataiku=DataikuConfig(
            url="https://fake-dataiku.example.com",
            api_key_env="DATAIKU_API_KEY",
            project_key="TEST_PROJECT",
        ),
        fabric=FabricConfig(
            workspace_id="00000000-0000-0000-0000-000000000000",
            lakehouse_name="lh_test",
            warehouse_name="wh_test",
        ),
        migration=MigrationConfig(
            output_dir=str(tmp_path / "output"),
            parallel_agents=False,
            fail_fast=False,
        ),
        orchestrator=OrchestratorConfig(max_retries=1, retry_delay_seconds=0),
        logging=LoggingConfig(level="DEBUG", format="console", file=str(tmp_path / "test.log")),
    )


@pytest.fixture
def registry(tmp_path) -> AssetRegistry:
    """Empty AssetRegistry backed by a temp file."""
    return AssetRegistry(project_key="TEST_PROJECT", registry_path=tmp_path / "registry.json")


@pytest.fixture
def sample_sql_asset() -> Asset:
    return Asset(
        id="recipe_sql_compute_orders",
        name="compute_orders",
        type=AssetType.RECIPE_SQL,
        source_project=PROJECT,
        state=MigrationState.DISCOVERED,
        metadata={
            "type": "sql",
            "payload": "SELECT NVL(a, b) FROM orders WHERE ROWNUM <= 10",
            "connection": "oracle_dw",
        },
    )


@pytest.fixture
def sample_python_asset() -> Asset:
    return Asset(
        id="recipe_python_transform",
        name="transform",
        type=AssetType.RECIPE_PYTHON,
        source_project=PROJECT,
        state=MigrationState.DISCOVERED,
        metadata={
            "type": "python",
            "payload": 'import dataiku\ndf = dataiku.Dataset("orders").get_dataframe()\ndf.to_csv("out.csv")',
        },
    )


@pytest.fixture
def sample_dataset_asset() -> Asset:
    return Asset(
        id="dataset_orders",
        name="orders",
        type=AssetType.DATASET,
        source_project=PROJECT,
        state=MigrationState.DISCOVERED,
        metadata={
            "type": "PostgreSQL",
            "schema": [
                {"name": "id", "type": "int"},
                {"name": "customer", "type": "string"},
                {"name": "amount", "type": "double"},
            ],
        },
    )


@pytest.fixture
def sample_connection_asset() -> Asset:
    return Asset(
        id="connection_oracle_dw",
        name="oracle_dw",
        type=AssetType.CONNECTION,
        source_project=PROJECT,
        state=MigrationState.DISCOVERED,
        metadata={"type": "Oracle"},
    )


@pytest.fixture
def populated_registry(registry, sample_sql_asset, sample_python_asset, sample_dataset_asset) -> AssetRegistry:
    """Registry pre-loaded with a few sample assets."""
    registry.add_asset(sample_sql_asset)
    registry.add_asset(sample_python_asset)
    registry.add_asset(sample_dataset_asset)
    return registry


# ── Realistic Dataiku API fixtures ─────────────────────────────


@pytest.fixture
def oracle_etl_recipe() -> dict:
    """Realistic Oracle SQL recipe with NVL, DECODE, SYSDATE, LISTAGG, ROWNUM."""
    return _load_json("recipes/oracle_etl_recipe.json")


@pytest.fixture
def oracle_hierarchical_recipe() -> dict:
    """Oracle CONNECT BY hierarchical query."""
    return _load_json("recipes/oracle_hierarchical_recipe.json")


@pytest.fixture
def postgres_analytics_recipe() -> dict:
    """PostgreSQL recipe with CTEs, ::cast, ILIKE, PERCENTILE_CONT, DISTINCT ON."""
    return _load_json("recipes/postgres_analytics_recipe.json")


@pytest.fixture
def postgres_multistatement_recipe() -> dict:
    """Multi-statement PostgreSQL recipe with CREATE TEMP TABLE + final SELECT."""
    return _load_json("recipes/postgres_multistatement_recipe.json")


@pytest.fixture
def python_feature_recipe() -> dict:
    """Python recipe with dataiku.Dataset, dataiku.Folder, get_custom_variables."""
    return _load_json("recipes/python_feature_engineering.json")


@pytest.fixture
def python_sdk_heavy_recipe() -> dict:
    """Python recipe with SQLExecutor2, api_client, heavy SDK usage."""
    return _load_json("recipes/python_sdk_heavy_recipe.json")


@pytest.fixture
def visual_join_recipe() -> dict:
    """Visual JOIN recipe with LEFT join and column conditions."""
    return _load_json("recipes/visual_join_recipe.json")


@pytest.fixture
def visual_group_recipe() -> dict:
    """Visual GROUP BY recipe with multiple aggregations."""
    return _load_json("recipes/visual_group_recipe.json")


@pytest.fixture
def visual_filter_recipe() -> dict:
    """Visual FILTER recipe with multiple conditions."""
    return _load_json("recipes/visual_filter_recipe.json")


@pytest.fixture
def visual_window_recipe() -> dict:
    """Visual WINDOW recipe with RANK partitioned by region."""
    return _load_json("recipes/visual_window_recipe.json")


@pytest.fixture
def visual_pivot_recipe() -> dict:
    """Visual PIVOT recipe with group columns and pivot values."""
    return _load_json("recipes/visual_pivot_recipe.json")


@pytest.fixture
def visual_prepare_recipe() -> dict:
    """Visual PREPARE recipe with 11 data wrangling steps."""
    return _load_json("recipes/visual_prepare_recipe.json")


@pytest.fixture
def sample_datasets() -> list[dict]:
    """5 realistic datasets: Oracle, PostgreSQL, Filesystem (managed + partitioned)."""
    return _load_json("datasets/datasets.json")


@pytest.fixture
def sample_connections() -> dict:
    """6 connections: Oracle, PostgreSQL, S3, Azure Blob, HDFS, Filesystem."""
    return _load_json("connections/connections.json")


@pytest.fixture
def sample_flow() -> dict:
    """Full project flow DAG with 16 nodes and 16 edges."""
    return _load_json("flows/project_flow.json")


@pytest.fixture
def sample_scenarios() -> list[dict]:
    """3 scenarios: daily cron, weekly cron, on-demand."""
    return _load_json("scenarios/scenarios.json")


@pytest.fixture
def sample_managed_folders() -> list[dict]:
    """3 managed folders: Filesystem + S3."""
    return _load_json("folders/managed_folders.json")


@pytest.fixture
def sample_saved_models() -> list[dict]:
    """2 saved models: binary classification + clustering."""
    return _load_json("models/saved_models.json")


@pytest.fixture
def sample_dashboards() -> list[dict]:
    """2 dashboards: Sales Overview + Customer Health."""
    return _load_json("dashboards/dashboards.json")
