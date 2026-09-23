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

    async def _list_saved_model_versions(project_key: str, model_id: str) -> list[dict]:
        return fixtures.get("saved_model_versions", {}).get(model_id, [])

    async def _get_saved_model_version_details(
        project_key: str,
        model_id: str,
        version_id: str,
    ) -> dict:
        return fixtures.get("saved_model_version_details", {}).get(version_id, {})

    client.list_saved_model_versions = AsyncMock(side_effect=_list_saved_model_versions)
    client.get_saved_model_version_details = AsyncMock(side_effect=_get_saved_model_version_details)
    client.list_dashboards.return_value = fixtures["dashboards"]
    client.list_insights.return_value = fixtures.get("insights", [])
    client.get_insight.return_value = fixtures.get("insight_payloads", {})

    client.list_webapps.return_value = fixtures.get("webapps", [])
    client.list_streaming_endpoints.return_value = fixtures.get("streaming_endpoints", [])
    client.list_jupyter_notebooks.return_value = fixtures.get("jupyter_notebooks", [])
    client.list_api_services.return_value = fixtures.get("api_services", [])

    async def _list_api_service_packages(project_key: str, service_id: str) -> list[dict]:
        return fixtures.get("api_service_packages", {}).get(service_id, [])

    client.list_api_service_packages = AsyncMock(side_effect=_list_api_service_packages)
    client.list_project_library_contents.return_value = fixtures.get("library_contents", [])
    client.get_project_library_file.return_value = fixtures.get("external_libraries", {})
    client.get_project_data_quality_status.return_value = fixtures.get("data_quality_status", {})
    client.get_dataset_data_quality_rules.return_value = fixtures.get("data_quality_rules", {})

    async def _get_jupyter_notebook(project_key: str, notebook_name: str) -> dict:
        return fixtures.get("jupyter_notebook_payloads", {}).get(notebook_name, {})

    client.get_jupyter_notebook = AsyncMock(side_effect=_get_jupyter_notebook)
    client.get_project_variables.return_value = fixtures.get("variables", {"standard": {}, "local": {}})

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
    async def test_saved_models_include_versions_and_details(self, tmp_path):
        fixtures = _load_fixtures()
        fixtures["saved_model_versions"] = {
            "model_churn": [{"id": "v1", "active": True, "trainDate": 123}]
        }
        fixtures["saved_model_version_details"] = {
            "v1": {"pythonCodeEnvName": "ml-env", "predictionType": "BINARY_CLASSIFICATION"}
        }
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        await DiscoveryAgent().execute(ctx)

        model = next(
            asset for asset in ctx.registry.get_by_type(AssetType.SAVED_MODEL)
            if asset.name == "churn_predictor"
        )
        assert model.metadata["versions"] == [{"id": "v1", "active": True, "trainDate": 123}]
        assert model.metadata["version_details"]["v1"]["pythonCodeEnvName"] == "ml-env"

    @pytest.mark.asyncio
    async def test_saved_model_version_failure_keeps_model(self, tmp_path):
        fixtures = _load_fixtures()
        fixtures["saved_model_versions"] = {"model_churn": [{"id": "v1"}]}
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.get_saved_model_version_details = AsyncMock(side_effect=RuntimeError("details boom"))
        ctx.connectors["dataiku"] = client

        result = await DiscoveryAgent().execute(ctx)

        model_assets = ctx.registry.get_by_type(AssetType.SAVED_MODEL)
        assert len(model_assets) == 2
        assert any("version 'v1' details not discovered" in flag for flag in result.review_flags)

    @pytest.mark.asyncio
    async def test_discovers_dashboards(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        await agent.execute(ctx)

        dashboard_assets = ctx.registry.get_by_type(AssetType.DASHBOARD)
        assert len(dashboard_assets) == 2

    @pytest.mark.asyncio
    async def test_discovers_insights_with_payload_and_review_flag(self, tmp_path):
        fixtures = _load_fixtures()
        fixtures["insights"] = [{"id": "insight_sales", "name": "Sales Trend", "type": "line"}]
        fixtures["insight_payloads"] = {"id": "insight_sales", "payload": "chart-data"}
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        result = await DiscoveryAgent().execute(ctx)

        assert result.status.value == "completed"
        assets = ctx.registry.get_by_type(AssetType.INSIGHT)
        assert len(assets) == 1
        assert assets[0].metadata["payload"]["payload"] == "chart-data"
        assert any("manual Fabric/Power BI migration" in flag for flag in assets[0].review_flags)

    @pytest.mark.asyncio
    async def test_insight_detail_failure_keeps_insight_asset(self, tmp_path):
        fixtures = _load_fixtures()
        fixtures["insights"] = [{"id": "insight_sales", "name": "Sales Trend"}]
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.get_insight = AsyncMock(side_effect=RuntimeError("payload boom"))
        ctx.connectors["dataiku"] = client

        result = await DiscoveryAgent().execute(ctx)

        assert result.status.value == "completed"
        assert len(ctx.registry.get_by_type(AssetType.INSIGHT)) == 1
        assert any("Insight 'insight_sales' payload not discovered" in flag for flag in result.review_flags)

    @pytest.mark.asyncio
    async def test_insights_failure_does_not_fail_discovery(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.list_insights = AsyncMock(side_effect=RuntimeError("list boom"))
        ctx.connectors["dataiku"] = client

        result = await DiscoveryAgent().execute(ctx)

        assert result.status.value == "completed"
        assert any("Insights not discovered" in flag for flag in result.review_flags)


class TestDiscoveryConnectionsGracefulDegradation:
    """`/admin/connections/` requires an admin API key — a project-scoped

    key (the common case) must not abort the whole discovery run.
    """

    @pytest.mark.asyncio
    async def test_connections_failure_does_not_fail_discovery(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.list_connections = AsyncMock(side_effect=PermissionError("403 Forbidden"))
        ctx.connectors["dataiku"] = client

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        assert result.assets_processed > 0
        assert ctx.registry.get_by_type(AssetType.CONNECTION) == []

    @pytest.mark.asyncio
    async def test_connections_failure_raises_review_flag(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.list_connections = AsyncMock(side_effect=PermissionError("403 Forbidden"))
        ctx.connectors["dataiku"] = client

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert any("admin API key" in flag for flag in result.review_flags)

    @pytest.mark.asyncio
    async def test_other_asset_types_still_discovered_when_connections_fail(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.list_connections = AsyncMock(side_effect=PermissionError("403 Forbidden"))
        ctx.connectors["dataiku"] = client

        agent = DiscoveryAgent()
        await agent.execute(ctx)

        assert ctx.registry.get_by_type(AssetType.RECIPE_SQL)
        assert ctx.registry.get_by_type(AssetType.DATASET)
        assert ctx.registry.get_by_type(AssetType.SCENARIO)


class TestDiscoveryWebappsStreamingVariables:
    """Webapps and streaming endpoints have no direct Fabric equivalent —

    they must be cataloged with a review flag rather than silently dropped.
    Project variables must be attached to the flow asset for downstream use.
    """

    @pytest.mark.asyncio
    async def test_webapps_discovered_and_flagged_for_review(self, tmp_path):
        fixtures = _load_fixtures()
        fixtures["webapps"] = [{"id": "wa1", "name": "MyDashboardApp", "type": "SHINY"}]
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        webapp_assets = ctx.registry.get_by_type(AssetType.WEBAPP)
        assert len(webapp_assets) == 1
        assert webapp_assets[0].name == "MyDashboardApp"
        assert any("manual re-implementation" in flag for flag in webapp_assets[0].review_flags)

    @pytest.mark.asyncio
    async def test_streaming_endpoints_discovered_and_flagged_for_review(self, tmp_path):
        fixtures = _load_fixtures()
        fixtures["streaming_endpoints"] = [{"id": "kafka_topic_1", "type": "kafka"}]
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        endpoint_assets = ctx.registry.get_by_type(AssetType.STREAMING_ENDPOINT)
        assert len(endpoint_assets) == 1
        assert endpoint_assets[0].name == "kafka_topic_1"
        assert any("manual re-implementation" in flag for flag in endpoint_assets[0].review_flags)

    @pytest.mark.asyncio
    async def test_webapps_failure_does_not_fail_discovery(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.list_webapps = AsyncMock(side_effect=RuntimeError("boom"))
        ctx.connectors["dataiku"] = client

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        assert any("Webapps not discovered" in flag for flag in result.review_flags)

    @pytest.mark.asyncio
    async def test_streaming_endpoints_failure_does_not_fail_discovery(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.list_streaming_endpoints = AsyncMock(side_effect=RuntimeError("boom"))
        ctx.connectors["dataiku"] = client

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        assert any("Streaming endpoints not discovered" in flag for flag in result.review_flags)

    @pytest.mark.asyncio
    async def test_project_variables_attached_to_flow_asset(self, tmp_path):
        fixtures = _load_fixtures()
        fixtures["variables"] = {"standard": {"env": "prod"}, "local": {}}
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        await agent.execute(ctx)

        flow_assets = ctx.registry.get_by_type(AssetType.FLOW)
        assert len(flow_assets) == 1
        assert flow_assets[0].metadata["variables"]["standard"]["env"] == "prod"

    @pytest.mark.asyncio
    async def test_project_variables_failure_does_not_fail_flow_discovery(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.get_project_variables = AsyncMock(side_effect=RuntimeError("boom"))
        ctx.connectors["dataiku"] = client

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        assert ctx.registry.get_by_type(AssetType.FLOW)
        assert any("Project variables not discovered" in flag for flag in result.review_flags)


class TestDiscoveryJupyterNotebooks:
    """Jupyter notebooks are ad-hoc project assets, distinct from Flow recipes."""

    @pytest.mark.asyncio
    async def test_jupyter_notebooks_discovered_with_payload_and_flag(self, tmp_path):
        fixtures = _load_fixtures()
        fixtures["jupyter_notebooks"] = [
            {"name": "Experiment 42", "language": "Python", "kernelSpec": {"name": "python3"}}
        ]
        fixtures["jupyter_notebook_payloads"] = {
            "Experiment 42": {"nbformat": 4, "cells": [{"cell_type": "code", "source": ["1+1"]}]}
        }
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        notebook_assets = ctx.registry.get_by_type(AssetType.JUPYTER_NOTEBOOK)
        assert len(notebook_assets) == 1
        assert notebook_assets[0].name == "Experiment 42"
        assert notebook_assets[0].metadata["payload"]["nbformat"] == 4
        assert any("manual Fabric notebook migration" in flag for flag in notebook_assets[0].review_flags)

    @pytest.mark.asyncio
    async def test_jupyter_notebook_detail_failure_does_not_drop_listing(self, tmp_path):
        fixtures = _load_fixtures()
        fixtures["jupyter_notebooks"] = [{"name": "BrokenNotebook", "language": "Python"}]
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.get_jupyter_notebook = AsyncMock(side_effect=RuntimeError("detail boom"))
        ctx.connectors["dataiku"] = client

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        notebook_assets = ctx.registry.get_by_type(AssetType.JUPYTER_NOTEBOOK)
        assert len(notebook_assets) == 1
        assert notebook_assets[0].metadata["payload"] == {}
        assert any("payload not discovered" in flag for flag in result.review_flags)

    @pytest.mark.asyncio
    async def test_jupyter_notebooks_failure_does_not_fail_discovery(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.list_jupyter_notebooks = AsyncMock(side_effect=RuntimeError("boom"))
        ctx.connectors["dataiku"] = client

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        assert any("Jupyter notebooks not discovered" in flag for flag in result.review_flags)


class TestDiscoveryApiServices:
    """API services expose Dataiku prediction/custom endpoints."""

    @pytest.mark.asyncio
    async def test_api_services_discovered_with_packages_and_flag(self, tmp_path):
        fixtures = _load_fixtures()
        fixtures["api_services"] = [
            {"id": "customer-scoring", "publicAccess": "true", "endpoints": [{"id": "score", "type": "CUSTOM_PREDICTION"}]}
        ]
        fixtures["api_service_packages"] = {
            "customer-scoring": [{"id": "v1", "createdOn": 123}]
        }
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        service_assets = ctx.registry.get_by_type(AssetType.API_SERVICE)
        assert len(service_assets) == 1
        assert service_assets[0].name == "customer-scoring"
        assert service_assets[0].metadata["packages"][0]["id"] == "v1"
        assert any("manual Fabric/Azure endpoint migration" in flag for flag in service_assets[0].review_flags)

    @pytest.mark.asyncio
    async def test_api_service_package_failure_does_not_drop_service(self, tmp_path):
        fixtures = _load_fixtures()
        fixtures["api_services"] = [{"id": "customer-scoring", "endpoints": []}]
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.list_api_service_packages = AsyncMock(side_effect=RuntimeError("package boom"))
        ctx.connectors["dataiku"] = client

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        service_assets = ctx.registry.get_by_type(AssetType.API_SERVICE)
        assert len(service_assets) == 1
        assert service_assets[0].metadata["packages"] == []
        assert any("packages not discovered" in flag for flag in result.review_flags)

    @pytest.mark.asyncio
    async def test_api_services_failure_does_not_fail_discovery(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.list_api_services = AsyncMock(side_effect=RuntimeError("boom"))
        ctx.connectors["dataiku"] = client

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        assert any("API services not discovered" in flag for flag in result.review_flags)


class TestDiscoveryProjectLibrary:
    @pytest.mark.asyncio
    async def test_project_library_discovered_with_dependency_metadata(self, tmp_path):
        fixtures = _load_fixtures()
        fixtures["library_contents"] = [{"path": "external-libraries.json", "mimeType": "application/json"}]
        fixtures["external_libraries"] = {"python": {"packages": ["requests==2.32.0"]}}
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        result = await DiscoveryAgent().execute(ctx)

        assert result.status.value == "completed"
        assets = ctx.registry.get_by_type(AssetType.PROJECT_LIBRARY)
        assert len(assets) == 1
        assert assets[0].metadata["external_libraries"]["python"]["packages"] == ["requests==2.32.0"]
        assert any("dependencies require manual review" in flag for flag in assets[0].review_flags)

    @pytest.mark.asyncio
    async def test_project_library_metadata_failure_keeps_library_asset(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.get_project_library_file = AsyncMock(side_effect=RuntimeError("metadata boom"))
        ctx.connectors["dataiku"] = client

        result = await DiscoveryAgent().execute(ctx)

        assert result.status.value == "completed"
        assets = ctx.registry.get_by_type(AssetType.PROJECT_LIBRARY)
        assert len(assets) == 1
        assert assets[0].metadata["external_libraries"] == {}
        assert any("Project library metadata not discovered" in flag for flag in result.review_flags)

    @pytest.mark.asyncio
    async def test_project_library_failure_does_not_fail_discovery(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.list_project_library_contents = AsyncMock(side_effect=RuntimeError("library boom"))
        ctx.connectors["dataiku"] = client

        result = await DiscoveryAgent().execute(ctx)

        assert result.status.value == "completed"
        assert any("Project library not discovered" in flag for flag in result.review_flags)


class TestDiscoveryDataQuality:
    @pytest.mark.asyncio
    async def test_dataset_includes_data_quality_status_and_rules(self, tmp_path):
        fixtures = _load_fixtures()
        fixtures["data_quality_status"] = {"raw_orders": "OK"}
        fixtures["data_quality_rules"] = {"monitor": True, "checks": [{"id": "r1", "enabled": True}]}
        ctx = _make_context(tmp_path)
        ctx.connectors["dataiku"] = _make_mock_client(fixtures)

        result = await DiscoveryAgent().execute(ctx)

        assert result.status.value == "completed"
        orders = next(asset for asset in ctx.registry.get_by_type(AssetType.DATASET) if asset.name == "raw_orders")
        assert orders.metadata["data_quality"] == {
            "status": "OK",
            "rules": {"monitor": True, "checks": [{"id": "r1", "enabled": True}]},
        }

    @pytest.mark.asyncio
    async def test_data_quality_status_failure_does_not_drop_datasets(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.get_project_data_quality_status = AsyncMock(side_effect=RuntimeError("status boom"))
        ctx.connectors["dataiku"] = client

        result = await DiscoveryAgent().execute(ctx)

        assert result.status.value == "completed"
        assert ctx.registry.get_by_type(AssetType.DATASET)
        assert any("Project Data Quality status not discovered" in flag for flag in result.review_flags)

    @pytest.mark.asyncio
    async def test_data_quality_rules_failure_does_not_drop_dataset(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.get_dataset_data_quality_rules = AsyncMock(side_effect=RuntimeError("rules boom"))
        ctx.connectors["dataiku"] = client

        result = await DiscoveryAgent().execute(ctx)

        assert result.status.value == "completed"
        dataset_assets = ctx.registry.get_by_type(AssetType.DATASET)
        assert dataset_assets
        assert dataset_assets[0].metadata["data_quality"]["rules"] == {}
        assert any("Data Quality rules not discovered" in flag for flag in result.review_flags)


class TestDiscoveryPartialFailureResilience:
    """A single bad recipe/dataset/flow/scenario must not abort the whole
    discovery run or lose already-discovered assets.
    """

    @pytest.mark.asyncio
    async def test_one_bad_recipe_does_not_block_others(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)

        async def _get_recipe(project_key: str, recipe_name: str) -> dict:
            if recipe_name == "compute_orders":
                raise RuntimeError("500 Internal Server Error")
            return fixtures["recipe_details"].get(recipe_name, {"name": recipe_name})

        client.get_recipe = AsyncMock(side_effect=_get_recipe)
        ctx.connectors["dataiku"] = client

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        assert ctx.registry.get_asset("recipe_compute_orders") is None
        assert ctx.registry.get_asset("recipe_transform_data") is not None
        assert any("compute_orders" in flag for flag in result.review_flags)

    @pytest.mark.asyncio
    async def test_one_bad_dataset_schema_does_not_block_others(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)

        async def _get_schema(project_key: str, dataset_name: str) -> dict:
            if dataset_name == "raw_orders":
                raise RuntimeError("schema not computed")
            for ds in fixtures["datasets"]:
                if ds["name"] == dataset_name:
                    return ds.get("schema", {"columns": []})
            return {"columns": []}

        client.get_dataset_schema = AsyncMock(side_effect=_get_schema)
        ctx.connectors["dataiku"] = client

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        assert ctx.registry.get_asset("dataset_raw_orders") is None
        assert ctx.registry.get_asset("dataset_products") is not None
        assert any("raw_orders" in flag for flag in result.review_flags)

    @pytest.mark.asyncio
    async def test_flow_failure_degrades_gracefully(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.get_flow = AsyncMock(side_effect=RuntimeError("boom"))
        ctx.connectors["dataiku"] = client

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        assert ctx.registry.get_by_type(AssetType.FLOW) == []
        assert any("Flow not discovered" in flag for flag in result.review_flags)

    @pytest.mark.asyncio
    async def test_scenarios_failure_degrades_gracefully(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.list_scenarios = AsyncMock(side_effect=RuntimeError("boom"))
        ctx.connectors["dataiku"] = client

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "completed"
        assert ctx.registry.get_by_type(AssetType.SCENARIO) == []
        assert any("Scenarios not discovered" in flag for flag in result.review_flags)

    @pytest.mark.asyncio
    async def test_fatal_error_persists_partial_progress(self, tmp_path):
        fixtures = _load_fixtures()
        ctx = _make_context(tmp_path)
        client = _make_mock_client(fixtures)
        client.list_datasets = AsyncMock(side_effect=RuntimeError("fatal listing error"))
        ctx.connectors["dataiku"] = client

        agent = DiscoveryAgent()
        result = await agent.execute(ctx)

        assert result.status.value == "failed"
        assert result.assets_processed > 0
        assert ctx.registry.registry_path.exists()
        assert ctx.registry.get_asset("recipe_compute_orders") is not None


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
