"""Flow → Pipeline agent — converts Dataiku flows and scenarios to Fabric Data Pipelines."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import networkx as nx

from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent, ValidationResult
from src.core.logger import get_logger
from src.models.asset import AssetType, MigrationState

logger = get_logger(__name__)


def build_flow_dag(flow_data: dict) -> nx.DiGraph:
    """Build a directed acyclic graph from Dataiku flow data."""
    graph = nx.DiGraph()
    items = flow_data.get("graph", {}).get("items", {})
    edges = flow_data.get("graph", {}).get("edges", [])

    for item_id, item in items.items():
        graph.add_node(item_id, **item)

    for edge in edges:
        source = edge.get("from", "")
        target = edge.get("to", "")
        if source and target:
            graph.add_edge(source, target)

    return graph


def _recipe_to_activity(recipe_name: str, recipe_type: str, depends_on: list[str]) -> dict:
    """Convert a Dataiku recipe to a Fabric Pipeline activity."""
    dependency_list = [
        {"activity": dep, "dependencyConditions": ["Succeeded"]}
        for dep in depends_on
    ]

    if recipe_type in ("python", "pyspark", "r"):
        return {
            "name": f"run_{recipe_name}",
            "type": "SynapseNotebook",
            "dependsOn": dependency_list,
            "typeProperties": {
                "notebook": {"referenceName": f"{recipe_name}_notebook"},
            },
        }
    else:
        # SQL and visual recipes → SQL script activity
        return {
            "name": f"run_{recipe_name}",
            "type": "SqlServerStoredProcedure",
            "dependsOn": dependency_list,
            "typeProperties": {
                "scriptPath": f"sql/{recipe_name}.sql",
            },
        }


def generate_pipeline_json(
    pipeline_name: str,
    flow_data: dict,
    recipe_metadata: dict[str, dict],
) -> dict:
    """Generate a Fabric Data Pipeline JSON definition from a Dataiku flow."""
    graph = build_flow_dag(flow_data)

    # Topological sort for execution order
    try:
        execution_order = list(nx.topological_sort(graph))
    except nx.NetworkXUnfeasible:
        logger.warning("flow_has_cycles", pipeline=pipeline_name)
        execution_order = list(graph.nodes())

    activities: list[dict] = []
    for node_id in execution_order:
        node_data = graph.nodes.get(node_id, {})
        recipe_name = node_data.get("name", node_id)
        recipe_type = node_data.get("type", "sql")

        # Find predecessor activities
        predecessors = list(graph.predecessors(node_id))
        depends_on = [f"run_{graph.nodes.get(p, {}).get('name', p)}" for p in predecessors]

        activity = _recipe_to_activity(recipe_name, recipe_type, depends_on)
        activities.append(activity)

    return {
        "name": pipeline_name,
        "properties": {
            "activities": activities,
            "annotations": [f"Migrated from Dataiku flow"],
        },
    }


class FlowPipelineAgent(BaseAgent):
    """Converts Dataiku flows and scenarios to Fabric Data Pipelines."""

    @property
    def name(self) -> str:
        return "pipeline_builder"

    @property
    def description(self) -> str:
        return "Convert Dataiku flows and scenarios to Fabric Data Pipeline JSON"

    async def execute(self, context: Any) -> AgentResult:
        registry = context.registry
        config = context.config
        output_dir = Path(config.migration.output_dir) / "pipelines"
        output_dir.mkdir(parents=True, exist_ok=True)

        flow_assets = registry.get_by_type(AssetType.FLOW)
        scenario_assets = registry.get_by_type(AssetType.SCENARIO)
        processed = 0
        converted = 0
        failed = 0
        errors: list[str] = []

        # Convert flows to pipelines
        for asset in flow_assets:
            registry.update_state(asset.id, MigrationState.CONVERTING)
            try:
                recipe_metadata = {}
                for recipe_asset in (
                    registry.get_by_type(AssetType.RECIPE_SQL)
                    + registry.get_by_type(AssetType.RECIPE_PYTHON)
                    + registry.get_by_type(AssetType.RECIPE_VISUAL)
                ):
                    recipe_metadata[recipe_asset.name] = recipe_asset.metadata

                pipeline_json = generate_pipeline_json(
                    pipeline_name=f"{asset.source_project}_pipeline",
                    flow_data=asset.metadata,
                    recipe_metadata=recipe_metadata,
                )

                out_file = output_dir / f"{asset.name}.json"
                out_file.write_text(json.dumps(pipeline_json, indent=2), encoding="utf-8")

                registry.set_target(asset.id, {
                    "type": "data_pipeline",
                    "path": str(out_file),
                    "activity_count": len(pipeline_json["properties"]["activities"]),
                })
                registry.update_state(asset.id, MigrationState.CONVERTED)
                converted += 1

            except Exception as e:
                logger.error("flow_conversion_error", asset=asset.id, error=str(e))
                registry.add_error(asset.id, str(e))
                registry.update_state(asset.id, MigrationState.FAILED)
                errors.append(f"{asset.name}: {e}")
                failed += 1

            processed += 1

        # Convert scenarios to pipeline triggers
        for asset in scenario_assets:
            registry.update_state(asset.id, MigrationState.CONVERTING)
            try:
                trigger_config = self._scenario_to_trigger(asset.metadata)
                out_file = output_dir / f"{asset.name}_trigger.json"
                out_file.write_text(json.dumps(trigger_config, indent=2), encoding="utf-8")

                registry.set_target(asset.id, {
                    "type": "pipeline_trigger",
                    "path": str(out_file),
                })
                registry.update_state(asset.id, MigrationState.CONVERTED)
                converted += 1

            except Exception as e:
                logger.error("scenario_conversion_error", asset=asset.id, error=str(e))
                registry.add_error(asset.id, str(e))
                registry.update_state(asset.id, MigrationState.FAILED)
                errors.append(f"{asset.name}: {e}")
                failed += 1

            processed += 1

        return AgentResult(
            agent_name=self.name,
            status=AgentStatus.COMPLETED,
            assets_processed=processed,
            assets_converted=converted,
            assets_failed=failed,
            errors=errors,
        )

    def _scenario_to_trigger(self, scenario_data: dict) -> dict:
        """Convert a Dataiku scenario to a pipeline trigger config."""
        triggers = scenario_data.get("triggers", [])
        trigger_config = {"triggers": []}

        for trigger in triggers:
            trigger_type = trigger.get("type", "").lower()
            if trigger_type == "temporal":
                trigger_config["triggers"].append({
                    "type": "ScheduleTrigger",
                    "properties": {
                        "recurrence": {
                            "frequency": trigger.get("frequency", "Day"),
                            "interval": trigger.get("interval", 1),
                        }
                    },
                })
            elif trigger_type == "dataset_change":
                trigger_config["triggers"].append({
                    "type": "EventTrigger",
                    "properties": {
                        "dataset": trigger.get("dataset", ""),
                    },
                })

        return trigger_config

    async def validate(self, context: Any) -> ValidationResult:
        registry = context.registry
        flow_assets = registry.get_by_type(AssetType.FLOW)
        converted = [a for a in flow_assets if a.state == MigrationState.CONVERTED]

        checks_run = 0
        failures: list[str] = []

        for asset in converted:
            target = asset.target_fabric_asset or {}
            pipeline_path = target.get("path")
            checks_run += 1
            if not pipeline_path or not Path(pipeline_path).exists():
                failures.append(f"{asset.name}: Pipeline JSON not found")
                continue

            try:
                content = json.loads(Path(pipeline_path).read_text(encoding="utf-8"))
                if "properties" not in content or "activities" not in content["properties"]:
                    failures.append(f"{asset.name}: Invalid pipeline structure")
            except json.JSONDecodeError as e:
                failures.append(f"{asset.name}: Invalid JSON — {e}")

        return ValidationResult(
            passed=len(failures) == 0,
            checks_run=checks_run,
            checks_passed=checks_run - len(failures),
            checks_failed=len(failures),
            failures=failures,
        )
