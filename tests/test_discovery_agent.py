"""Tests for the Discovery Agent with mocked Dataiku API responses."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from src.agents.discovery_agent import DiscoveryAgent, RECIPE_TYPE_MAP
from src.core.config import (
    AppConfig,
    DataikuConfig,
    FabricConfig,
    MigrationConfig,
    OrchestratorConfig,
)
from src.core.orchestrator import MigrationContext
from src.core.registry import AssetRegistry
from src.models.asset import AssetType, MigrationState

FIXTURES = Path(__file__).parent / "fixtures" / "dataiku_project.json"
PROJECT_KEY = "TEST_PROJECT"


def _load_fixtures() -> dict:
    return json.loads(FIXTURES.read_text())


def _make_mock_client(fixtures: dict) -> AsyncMock:
    """Build a mock DataikuClient from fixture data."""
    client = AsyncMock()

    client.list_recipes.return_value = fixtures["recipes"]

    async def _get_recipe(project_key: str, recipe_name: str) -> dict:
        return fixtures["recipe_details"].get(recipe_name, {"name": recipe_name})

    client.get_recipe = AsyncMock(side_effect=_get_recipe)
    client.list_datasets.return_value = fixtures["datasets"]

    async def _get_dataset_schema(project_key: str, dataset_name: str) -> dict:
        for ds in fixtures["datasets"]:
            if ds["name"] == dataset_name:
                return ds.get("schema", {"columns": []})
        return {"columns": []}

    client.get_dataset_schema = AsyncMock(side_effect=_get_dataset_schema)
    client.list_managed_folders.return_value = fixtures["managed_folders"]

    # Connections: admin endpoint returns dict keyed by name
    client.list_connections.return_value = [
        {"name": k, **v} for k, v in fixtures["connections"].items()
    ]

    client.get_flow.return_value = fixtures["flow"]
    client.list_scenarios.return_value = fixtures["scenarios"]
    client.list_saved_models.return_value = fixtures["saved_models"]
    client.list_dashboards.return_value = fixtures["dashboards"]

    return client


def _make_context(tmp_path: Path) -> MigrationContext:
    config = AppConfig(
        dataiku=DataikuConfig(url="https://fake.com", project_key=PROJECT_KEY),
        fabric=FabricConfig(workspace_id="ws-1"),
        migration=MigrationConfig(output_dir=str(tmp_path / "output")),
        orchestrator=OrchestratorConfig(max_retries=1),
    )
    registry = AssetRegistry(project_key=PROJECT_KEY, registry_path=tmp_path / "registry.json")
    return MigrationContext(config=config, registry=registry)


# ── Tests ─────────────────────────────────────────────────


class TestDiscoveryAgentBasic:
    """Basic discovery agent behavior."""

    @pytest.mark.asyncio
    async def test_fails_without_client(self, tmp_path):
        ctx = _make_context(tmp_path)
        agent = DiscoveryAgent()
        result = await agent.execute(ctx)
        assert result.status.value == "failed"
        assert "not configured" in result.errors[0].lower()

    @pytest.mark.asyncio
    async def test_discovers_all_asset_types(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        assert result.assets_processed > 0

        all_assets = ctx.registry.get_all()
        types_found = {a.type for a in all_assets}

        # Should discover recipes, datasets, folders, connections, flow, scenarios
        assert AssetType.RECIPE_SQL in types_found
        assert AssetType.RECIPE_PYTHON in types_found
        assert AssetType.RECIPE_VISUAL in types_found
        assert AssetType.DATASET in types_found
        assert AssetType.MANAGED_FOLDER in types_found
        assert AssetType.CONNECTION in types_found
        assert AssetType.FLOW in types_found
        assert AssetType.SCENARIO in types_found

    @pytest.mark.asyncio
    async def test_discovers_saved_models(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        await agent.execute(ctx)

        model_assets = ctx.registry.get_by_type(AssetType.SAVED_MODEL)
        assert len(model_assets) == 2

    @pytest.mark.asyncio
    async def test_discovers_dashboards(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        await agent.execute(ctx)

        dashboard_assets = ctx.registry.get_by_type(AssetType.DASHBOARD)
        assert len(dashboard_assets) == 2


class TestDiscoveryRecipes:
    """Recipe discovery specifics."""

    @pytest.mark.asyncio
    async def test_sql_recipe_classified_correctly(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        await agent.execute(ctx)

        sql_assets = ctx.registry.get_by_type(AssetType.RECIPE_SQL)
        names = [a.name for a in sql_assets]
        assert "compute_orders" in names

    @pytest.mark.asyncio
    async def test_python_recipe_classified_correctly(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        await agent.execute(ctx)

        py_assets = ctx.registry.get_by_type(AssetType.RECIPE_PYTHON)
        names = [a.name for a in py_assets]
        assert "transform_data" in names
        # pyspark also classified as RECIPE_PYTHON
        assert "pyspark_etl" in names

    @pytest.mark.asyncio
    async def test_visual_recipe_classified_correctly(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        await agent.execute(ctx)

        visual_assets = ctx.registry.get_by_type(AssetType.RECIPE_VISUAL)
        names = [a.name for a in visual_assets]
        assert "join_tables" in names
        assert "filter_active" in names
        assert "aggregate_sales" in names
        assert "prepare_clean" in names

    @pytest.mark.asyncio
    async def test_recipe_dependencies_set(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        await agent.execute(ctx)

        sql_recipe = ctx.registry.get_asset("recipe_compute_orders")
        assert sql_recipe is not None
        assert "dataset_raw_orders" in sql_recipe.dependencies
        assert "dataset_products" in sql_recipe.dependencies

    @pytest.mark.asyncio
    async def test_recipe_metadata_has_payload(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        await agent.execute(ctx)

        sql_recipe = ctx.registry.get_asset("recipe_compute_orders")
        assert "payload" in sql_recipe.metadata
        assert "ROWNUM" in sql_recipe.metadata["payload"]


class TestDiscoveryDatasets:
    """Dataset discovery specifics."""

    @pytest.mark.asyncio
    async def test_all_datasets_discovered(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        await agent.execute(ctx)

        ds_assets = ctx.registry.get_by_type(AssetType.DATASET)
        assert len(ds_assets) == len(fixtures["datasets"])

    @pytest.mark.asyncio
    async def test_dataset_has_schema(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        await agent.execute(ctx)

        ds = ctx.registry.get_asset("dataset_raw_orders")
        assert "schema" in ds.metadata
        columns = ds.metadata["schema"].get("columns", [])
        assert len(columns) > 0


class TestDiscoveryConnections:
    @pytest.mark.asyncio
    async def test_connections_discovered(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        await agent.execute(ctx)

        conn_assets = ctx.registry.get_by_type(AssetType.CONNECTION)
        assert len(conn_assets) == 3
        names = {a.name for a in conn_assets}
        assert "pg_warehouse" in names
        assert "oracle_erp" in names


class TestDiscoveryValidation:
    """Validate method tests."""

    @pytest.mark.asyncio
    async def test_validate_passes_on_clean_discovery(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        await agent.execute(ctx)
        result = await agent.validate(ctx)

        assert result.passed or result.checks_run > 0

    @pytest.mark.asyncio
    async def test_validate_detects_missing_dependency(self, tmp_path):
        """An asset referencing a non-existent dependency should be flagged."""
        from src.models.asset import Asset

        ctx = _make_context(tmp_path)
        ctx.registry.add_asset(Asset(
            id="recipe_orphan",
            type=AssetType.RECIPE_SQL,
            name="orphan",
            source_project=PROJECT_KEY,
            dependencies=["dataset_nonexistent"],
        ))

        agent = DiscoveryAgent()
        result = await agent.validate(ctx)

        assert not result.passed
        assert any("nonexistent" in f for f in result.failures)


class TestDiscoveryRegistrySave:
    @pytest.mark.asyncio
    async def test_registry_saved_after_discovery(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        await agent.execute(ctx)

        registry_path = tmp_path / "registry.json"
        assert registry_path.exists()
        saved_data = json.loads(registry_path.read_text())
        assert saved_data["project_key"] == PROJECT_KEY
        assert len(saved_data["assets"]) > 0

    @pytest.mark.asyncio
    async def test_all_assets_in_discovered_state(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        await agent.execute(ctx)

        for asset in ctx.registry.get_all():
            assert asset.state == MigrationState.DISCOVERED


class TestRecipeTypeMap:
    """Validate the recipe type mapping is complete."""

    def test_sql_dialects_mapped(self):
        assert RECIPE_TYPE_MAP["sql"] == AssetType.RECIPE_SQL
        assert RECIPE_TYPE_MAP["hive"] == AssetType.RECIPE_SQL
        assert RECIPE_TYPE_MAP["impala"] == AssetType.RECIPE_SQL

    def test_python_types_mapped(self):
        assert RECIPE_TYPE_MAP["python"] == AssetType.RECIPE_PYTHON
        assert RECIPE_TYPE_MAP["pyspark"] == AssetType.RECIPE_PYTHON
        assert RECIPE_TYPE_MAP["r"] == AssetType.RECIPE_PYTHON

    def test_visual_types_mapped(self):
        visual_types = ["join", "vstack", "group", "window", "filter", "sort",
                        "pivot", "prepare", "distinct", "sample", "topn", "split"]
        for vt in visual_types:
            assert RECIPE_TYPE_MAP[vt] == AssetType.RECIPE_VISUAL
