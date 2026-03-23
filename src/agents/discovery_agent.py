"""Discovery agent — scans a Dataiku project and catalogs all assets."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent, ValidationResult
from src.core.logger import get_logger
from src.models.asset import Asset, AssetType, MigrationState

logger = get_logger(__name__)

# Maps Dataiku recipe type strings to AssetType enum values.
RECIPE_TYPE_MAP: dict[str, AssetType] = {
    "sql": AssetType.RECIPE_SQL,
    "hive": AssetType.RECIPE_SQL,
    "impala": AssetType.RECIPE_SQL,
    "python": AssetType.RECIPE_PYTHON,
    "pyspark": AssetType.RECIPE_PYTHON,
    "r": AssetType.RECIPE_PYTHON,
    "join": AssetType.RECIPE_VISUAL,
    "vstack": AssetType.RECIPE_VISUAL,
    "group": AssetType.RECIPE_VISUAL,
    "window": AssetType.RECIPE_VISUAL,
    "filter": AssetType.RECIPE_VISUAL,
    "sort": AssetType.RECIPE_VISUAL,
    "pivot": AssetType.RECIPE_VISUAL,
    "prepare": AssetType.RECIPE_VISUAL,
    "distinct": AssetType.RECIPE_VISUAL,
    "sample": AssetType.RECIPE_VISUAL,
    "topn": AssetType.RECIPE_VISUAL,
    "split": AssetType.RECIPE_VISUAL,
}


class DiscoveryAgent(BaseAgent):
    """Scans a Dataiku project and registers all discoverable assets."""

    @property
    def name(self) -> str:
        return "discovery"

    @property
    def description(self) -> str:
        return "Scan Dataiku project via API and catalog all assets into the registry"

    async def execute(self, context: Any) -> AgentResult:
        """Discover all assets in the Dataiku project."""
        config = context.config
        registry = context.registry
        client = context.connectors.get("dataiku")

        if not client:
            return AgentResult(
                agent_name=self.name,
                status=AgentStatus.FAILED,
                errors=["Dataiku client not configured"],
            )

        project_key = config.dataiku.project_key
        processed = 0
        errors: list[str] = []

        try:
            # Discover recipes
            recipes = await client.list_recipes(project_key)
            for recipe in recipes:
                recipe_type = recipe.get("type", "").lower()
                asset_type = RECIPE_TYPE_MAP.get(recipe_type)
                if not asset_type:
                    logger.warning("unknown_recipe_type", type=recipe_type, name=recipe.get("name"))
                    continue

                detail = await client.get_recipe(project_key, recipe["name"])
                inputs = [ref.get("ref", "") for ref in detail.get("inputs", {}).get("main", {}).get("items", [])]
                outputs = [ref.get("ref", "") for ref in detail.get("outputs", {}).get("main", {}).get("items", [])]

                asset = Asset(
                    id=f"recipe_{recipe['name']}",
                    type=asset_type,
                    name=recipe["name"],
                    source_project=project_key,
                    state=MigrationState.DISCOVERED,
                    metadata={
                        "recipe_type": recipe_type,
                        "inputs": inputs,
                        "outputs": outputs,
                        **detail,
                    },
                    dependencies=[f"dataset_{ds}" for ds in inputs],
                    timestamps={"discovered_at": datetime.now(timezone.utc)},
                )
                registry.add_asset(asset)
                processed += 1

            # Discover datasets
            datasets = await client.list_datasets(project_key)
            for ds in datasets:
                schema = await client.get_dataset_schema(project_key, ds["name"])
                asset = Asset(
                    id=f"dataset_{ds['name']}",
                    type=AssetType.DATASET,
                    name=ds["name"],
                    source_project=project_key,
                    state=MigrationState.DISCOVERED,
                    metadata={**ds, "schema": schema},
                )
                registry.add_asset(asset)
                processed += 1

            # Discover managed folders
            folders = await client.list_managed_folders(project_key)
            for folder in folders:
                asset = Asset(
                    id=f"folder_{folder['name']}",
                    type=AssetType.MANAGED_FOLDER,
                    name=folder["name"],
                    source_project=project_key,
                    state=MigrationState.DISCOVERED,
                    metadata=folder,
                )
                registry.add_asset(asset)
                processed += 1

            # Discover connections
            connections = await client.list_connections()
            for conn in connections:
                asset = Asset(
                    id=f"connection_{conn.get('name', conn.get('id', ''))}",
                    type=AssetType.CONNECTION,
                    name=conn.get("name", conn.get("id", "")),
                    source_project=project_key,
                    state=MigrationState.DISCOVERED,
                    metadata=conn,
                )
                registry.add_asset(asset)
                processed += 1

            # Discover flow
            flow = await client.get_flow(project_key)
            asset = Asset(
                id=f"flow_{project_key}",
                type=AssetType.FLOW,
                name=f"{project_key}_flow",
                source_project=project_key,
                state=MigrationState.DISCOVERED,
                metadata=flow,
            )
            registry.add_asset(asset)
            processed += 1

            # Discover scenarios
            scenarios = await client.list_scenarios(project_key)
            for scenario in scenarios:
                asset = Asset(
                    id=f"scenario_{scenario['id']}",
                    type=AssetType.SCENARIO,
                    name=scenario.get("name", scenario["id"]),
                    source_project=project_key,
                    state=MigrationState.DISCOVERED,
                    metadata=scenario,
                )
                registry.add_asset(asset)
                processed += 1

            # Discover saved models
            try:
                models = await client.list_saved_models(project_key)
                for model in models:
                    asset = Asset(
                        id=f"model_{model.get('id', model.get('name', ''))}",
                        type=AssetType.SAVED_MODEL,
                        name=model.get("name", model.get("id", "")),
                        source_project=project_key,
                        state=MigrationState.DISCOVERED,
                        metadata=model,
                    )
                    registry.add_asset(asset)
                    processed += 1
            except Exception as e:
                logger.warning("saved_models_discovery_failed", error=str(e))

            # Discover dashboards
            try:
                dashboards = await client.list_dashboards(project_key)
                for dashboard in dashboards:
                    asset = Asset(
                        id=f"dashboard_{dashboard.get('id', dashboard.get('name', ''))}",
                        type=AssetType.DASHBOARD,
                        name=dashboard.get("name", dashboard.get("id", "")),
                        source_project=project_key,
                        state=MigrationState.DISCOVERED,
                        metadata=dashboard,
                    )
                    registry.add_asset(asset)
                    processed += 1
            except Exception as e:
                logger.warning("dashboards_discovery_failed", error=str(e))

            registry.save()
            logger.info("discovery_complete", project=project_key, assets=processed)

            return AgentResult(
                agent_name=self.name,
                status=AgentStatus.COMPLETED,
                assets_processed=processed,
                assets_converted=processed,
            )

        except Exception as e:
            logger.error("discovery_failed", error=str(e))
            return AgentResult(
                agent_name=self.name,
                status=AgentStatus.FAILED,
                assets_processed=processed,
                errors=[str(e)],
            )

    async def validate(self, context: Any) -> ValidationResult:
        """Verify discovery results are consistent."""
        registry = context.registry
        assets = registry.get_all()
        checks_run = 0
        failures: list[str] = []

        # Check all assets have valid types
        checks_run += 1
        for asset in assets:
            if asset.type not in AssetType:
                failures.append(f"Invalid asset type: {asset.type} for {asset.id}")

        # Check all dependencies reference existing assets
        checks_run += 1
        all_ids = {a.id for a in assets}
        for asset in assets:
            for dep in asset.dependencies:
                if dep not in all_ids:
                    failures.append(f"Unresolved dependency: {dep} in {asset.id}")

        return ValidationResult(
            passed=len(failures) == 0,
            checks_run=checks_run,
            checks_passed=checks_run - len(failures),
            checks_failed=len(failures),
            failures=failures,
        )
