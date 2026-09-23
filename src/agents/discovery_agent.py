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
        review_flags: list[str] = []

        try:
            # Discover recipes. Each recipe's detail is fetched individually,
            # so one bad/inaccessible recipe must not abort discovery of the
            # rest — skip it and flag it for review instead.
            recipes = await client.list_recipes(project_key)
            for recipe in recipes:
                recipe_type = recipe.get("type", "").lower()
                asset_type = RECIPE_TYPE_MAP.get(recipe_type)
                if not asset_type:
                    logger.warning("unknown_recipe_type", type=recipe_type, name=recipe.get("name"))
                    continue

                try:
                    detail = await client.get_recipe(project_key, recipe["name"])
                except Exception as e:
                    logger.warning("recipe_discovery_failed", recipe=recipe.get("name"), error=str(e))
                    review_flags.append(f"Recipe '{recipe.get('name')}' not discovered: {e}")
                    continue

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

            # Discover datasets. Schema retrieval can fail independently of
            # the dataset listing (e.g. schema not yet computed) — skip that
            # one dataset rather than losing everything discovered so far.
            datasets = await client.list_datasets(project_key)
            data_quality_status: dict = {}
            try:
                data_quality_status = await client.get_project_data_quality_status(project_key)
            except Exception as e:
                logger.warning("project_data_quality_status_discovery_failed", error=str(e))
                review_flags.append(f"Project Data Quality status not discovered: {e}")

            for ds in datasets:
                try:
                    schema = await client.get_dataset_schema(project_key, ds["name"])
                except Exception as e:
                    logger.warning("dataset_schema_discovery_failed", dataset=ds.get("name"), error=str(e))
                    review_flags.append(f"Dataset '{ds.get('name')}' schema not discovered: {e}")
                    continue

                quality_rules: dict = {}
                try:
                    quality_rules = await client.get_dataset_data_quality_rules(
                        project_key,
                        ds["name"],
                    )
                except Exception as e:
                    logger.warning(
                        "dataset_data_quality_rules_discovery_failed",
                        dataset=ds.get("name"),
                        error=str(e),
                    )
                    review_flags.append(f"Dataset '{ds.get('name')}' Data Quality rules not discovered: {e}")

                asset = Asset(
                    id=f"dataset_{ds['name']}",
                    type=AssetType.DATASET,
                    name=ds["name"],
                    source_project=project_key,
                    state=MigrationState.DISCOVERED,
                    metadata={
                        **ds,
                        "schema": schema,
                        "data_quality": {
                            "status": data_quality_status.get(ds["name"]),
                            "rules": quality_rules,
                        },
                    },
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

            # Discover connections — this typically requires an admin-level
            # API key, unlike everything else discovered so far. Degrade
            # gracefully so a project-scoped key doesn't abort the whole run.
            try:
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
            except Exception as e:
                logger.warning("connections_discovery_failed", error=str(e))
                review_flags.append(
                    f"Connections not discovered (requires admin API key): {e}"
                )

            # Discover flow — non-critical, degrade gracefully. Project
            # variables (used for ${var} substitution in recipes/scenarios)
            # are attached here so downstream translators/reports can surface
            # unresolved references instead of silently ignoring them.
            try:
                flow = await client.get_flow(project_key)
                variables: dict = {}
                try:
                    variables = await client.get_project_variables(project_key)
                except Exception as e:
                    logger.warning("project_variables_discovery_failed", error=str(e))
                    review_flags.append(f"Project variables not discovered: {e}")

                asset = Asset(
                    id=f"flow_{project_key}",
                    type=AssetType.FLOW,
                    name=f"{project_key}_flow",
                    source_project=project_key,
                    state=MigrationState.DISCOVERED,
                    metadata={**flow, "variables": variables},
                )
                registry.add_asset(asset)
                processed += 1
            except Exception as e:
                logger.warning("flow_discovery_failed", error=str(e))
                review_flags.append(f"Flow not discovered: {e}")

            # Discover scenarios — non-critical, degrade gracefully.
            try:
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
            except Exception as e:
                logger.warning("scenarios_discovery_failed", error=str(e))
                review_flags.append(f"Scenarios not discovered: {e}")

            # Discover saved models
            try:
                models = await client.list_saved_models(project_key)
                for model in models:
                    model_id = model.get("id", model.get("name", ""))
                    versions: list[dict] = []
                    version_details: dict[str, dict] = {}
                    try:
                        versions = await client.list_saved_model_versions(project_key, model_id)
                        for version in versions:
                            version_id = version.get("id", "")
                            if not version_id:
                                continue
                            try:
                                version_details[version_id] = await client.get_saved_model_version_details(
                                    project_key,
                                    model_id,
                                    version_id,
                                )
                            except Exception as e:
                                logger.warning(
                                    "saved_model_version_details_discovery_failed",
                                    model=model_id,
                                    version=version_id,
                                    error=str(e),
                                )
                                review_flags.append(
                                    f"Saved model '{model_id}' version '{version_id}' details not discovered: {e}"
                                )
                    except Exception as e:
                        logger.warning(
                            "saved_model_versions_discovery_failed",
                            model=model_id,
                            error=str(e),
                        )
                        review_flags.append(f"Saved model '{model_id}' versions not discovered: {e}")

                    asset = Asset(
                        id=f"model_{model_id}",
                        type=AssetType.SAVED_MODEL,
                        name=model.get("name", model_id),
                        source_project=project_key,
                        state=MigrationState.DISCOVERED,
                        metadata={
                            **model,
                            "versions": versions,
                            "version_details": version_details,
                        },
                    )
                    registry.add_asset(asset)
                    processed += 1
            except Exception as e:
                logger.warning("saved_models_discovery_failed", error=str(e))
                review_flags.append(f"Saved models not discovered: {e}")

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
                review_flags.append(f"Dashboards not discovered: {e}")

            # Discover webapps — Dataiku webapps (Shiny/Bokeh/Standard) have
            # no Fabric equivalent; catalog them but flag for manual review.
            try:
                webapps = await client.list_webapps(project_key)
                for webapp in webapps:
                    asset = Asset(
                        id=f"webapp_{webapp.get('id', webapp.get('name', ''))}",
                        type=AssetType.WEBAPP,
                        name=webapp.get("name", webapp.get("id", "")),
                        source_project=project_key,
                        state=MigrationState.DISCOVERED,
                        metadata=webapp,
                        review_flags=["No direct Fabric equivalent — requires manual re-implementation"],
                    )
                    registry.add_asset(asset)
                    processed += 1
            except Exception as e:
                logger.warning("webapps_discovery_failed", error=str(e))
                review_flags.append(f"Webapps not discovered: {e}")

            # Discover streaming endpoints (Kafka/etc.) — flagged the same way.
            try:
                endpoints = await client.list_streaming_endpoints(project_key)
                for endpoint in endpoints:
                    asset = Asset(
                        id=f"streaming_{endpoint.get('id', '')}",
                        type=AssetType.STREAMING_ENDPOINT,
                        name=endpoint.get("id", ""),
                        source_project=project_key,
                        state=MigrationState.DISCOVERED,
                        metadata=endpoint,
                        review_flags=["No direct Fabric equivalent — requires manual re-implementation"],
                    )
                    registry.add_asset(asset)
                    processed += 1
            except Exception as e:
                logger.warning("streaming_endpoints_discovery_failed", error=str(e))
                review_flags.append(f"Streaming endpoints not discovered: {e}")

            # Discover Jupyter notebooks — ad-hoc notebooks are outside the
            # Flow recipe graph, so catalog them with full nbformat payload and
            # flag them for manual migration planning.
            try:
                notebooks = await client.list_jupyter_notebooks(project_key)
                for notebook in notebooks:
                    notebook_name = notebook.get("name", "")
                    notebook_payload: dict = {}
                    try:
                        notebook_payload = await client.get_jupyter_notebook(project_key, notebook_name)
                    except Exception as e:
                        logger.warning(
                            "jupyter_notebook_detail_discovery_failed",
                            notebook=notebook_name,
                            error=str(e),
                        )
                        review_flags.append(f"Jupyter notebook '{notebook_name}' payload not discovered: {e}")

                    asset = Asset(
                        id=f"jupyter_notebook_{notebook_name}",
                        type=AssetType.JUPYTER_NOTEBOOK,
                        name=notebook_name,
                        source_project=project_key,
                        state=MigrationState.DISCOVERED,
                        metadata={"listing": notebook, "payload": notebook_payload},
                        review_flags=["Ad-hoc notebook outside Dataiku Flow — review for manual Fabric notebook migration"],
                    )
                    registry.add_asset(asset)
                    processed += 1
            except Exception as e:
                logger.warning("jupyter_notebooks_discovery_failed", error=str(e))
                review_flags.append(f"Jupyter notebooks not discovered: {e}")

            # Discover API services — these are deployable Dataiku prediction
            # or custom endpoints. Fabric/Power BI has no direct item type for
            # them, but losing them would hide production serving dependencies.
            try:
                api_services = await client.list_api_services(project_key)
                for service in api_services:
                    service_id = service.get("id", "")
                    packages: list[dict] = []
                    try:
                        packages = await client.list_api_service_packages(project_key, service_id)
                    except Exception as e:
                        logger.warning(
                            "api_service_packages_discovery_failed",
                            service=service_id,
                            error=str(e),
                        )
                        review_flags.append(f"API service '{service_id}' packages not discovered: {e}")

                    asset = Asset(
                        id=f"api_service_{service_id}",
                        type=AssetType.API_SERVICE,
                        name=service_id,
                        source_project=project_key,
                        state=MigrationState.DISCOVERED,
                        metadata={"service": service, "packages": packages},
                        review_flags=["Deployable API endpoint — requires manual Fabric/Azure endpoint migration"],
                    )
                    registry.add_asset(asset)
                    processed += 1
            except Exception as e:
                logger.warning("api_services_discovery_failed", error=str(e))
                review_flags.append(f"API services not discovered: {e}")

            # Discover the project library. Its external-libraries.json file
            # can contain dependencies needed to reproduce Python execution.
            try:
                library_contents = await client.list_project_library_contents(project_key)
                library_metadata: dict = {}
                try:
                    library_metadata = await client.get_project_library_file(
                        project_key,
                        "external-libraries.json",
                    )
                except Exception as e:
                    logger.warning("project_library_metadata_discovery_failed", error=str(e))
                    review_flags.append(f"Project library metadata not discovered: {e}")

                asset = Asset(
                    id=f"project_library_{project_key}",
                    type=AssetType.PROJECT_LIBRARY,
                    name=f"{project_key}_library",
                    source_project=project_key,
                    state=MigrationState.DISCOVERED,
                    metadata={"contents": library_contents, "external_libraries": library_metadata},
                    review_flags=["Project library dependencies require manual review for Fabric/Python migration"],
                )
                registry.add_asset(asset)
                processed += 1
            except Exception as e:
                logger.warning("project_library_discovery_failed", error=str(e))
                review_flags.append(f"Project library not discovered: {e}")

            registry.save()
            logger.info("discovery_complete", project=project_key, assets=processed)

            return AgentResult(
                agent_name=self.name,
                status=AgentStatus.COMPLETED,
                assets_processed=processed,
                assets_converted=processed,
                review_flags=review_flags,
            )

        except Exception as e:
            logger.error("discovery_failed", error=str(e))
            # Best-effort: persist whatever was discovered before the fatal
            # error so a re-run isn't starting from zero.
            try:
                if processed:
                    registry.save()
            except Exception:
                pass
            return AgentResult(
                agent_name=self.name,
                status=AgentStatus.FAILED,
                assets_processed=processed,
                errors=[str(e)],
                review_flags=review_flags,
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
