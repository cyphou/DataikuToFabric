"""Shared pytest fixtures for the Dataiku → Fabric Migration tests."""

from __future__ import annotations

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
