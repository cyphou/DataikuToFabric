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


# ── DAG construction ──────────────────────────────────────────

def build_flow_dag(flow_data: dict) -> nx.DiGraph:
    """Build a directed acyclic graph from Dataiku flow data.

    Dataiku flows have two main structures:
    - ``graph.items``: nodes (recipes, datasets) keyed by ID
    - ``graph.edges``: list of ``{from, to}`` dicts

    Returns a networkx DiGraph with node attributes from the items.
    """
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


def get_execution_order(graph: nx.DiGraph) -> list[str]:
    """Return nodes in topological order; fall back to BFS order on cycles."""
    try:
        return list(nx.topological_sort(graph))
    except nx.NetworkXUnfeasible:
        logger.warning("flow_has_cycles")
        return list(graph.nodes())


# ── Activity generation ───────────────────────────────────────

def _recipe_to_activity(
    recipe_name: str,
    recipe_type: str,
    depends_on: list[str],
    converted_asset: dict | None = None,
) -> dict:
    """Convert a Dataiku recipe to a Fabric Pipeline activity.

    Activity types:
    - Python/PySpark/R recipes → SynapseNotebook
    - SQL/visual recipes → Script (SQL)
    - Unknown → placeholder with review flag

    If ``converted_asset`` is provided, the activity references the actual
    converted notebook or SQL file path.
    """
    dependency_list = [
        {"activity": dep, "dependencyConditions": ["Succeeded"]}
        for dep in depends_on
    ]

    activity_name = f"run_{recipe_name}"

    if recipe_type in ("python", "pyspark", "r"):
        notebook_ref = recipe_name
        if converted_asset:
            notebook_ref = converted_asset.get("notebook_name", recipe_name)
        return {
            "name": activity_name,
            "type": "SynapseNotebook",
            "dependsOn": dependency_list,
            "typeProperties": {
                "notebook": {"referenceName": f"{notebook_ref}_notebook"},
            },
            "policy": {
                "timeout": "01:00:00",
                "retry": 1,
                "retryIntervalInSeconds": 30,
            },
        }

    if recipe_type in ("sql", "sql_query", "sql_script", "visual",
                        "join", "group", "filter", "window", "sort",
                        "distinct", "topn", "vstack", "pivot", "prepare"):
        script_path = f"sql/{recipe_name}.sql"
        if converted_asset:
            script_path = converted_asset.get("path", script_path)
        return {
            "name": activity_name,
            "type": "Script",
            "dependsOn": dependency_list,
            "typeProperties": {
                "scriptPath": script_path,
                "scriptType": "SQL",
            },
            "policy": {
                "timeout": "00:30:00",
                "retry": 1,
                "retryIntervalInSeconds": 30,
            },
        }

    # Unknown recipe type — placeholder
    return {
        "name": activity_name,
        "type": "Placeholder",
        "dependsOn": dependency_list,
        "typeProperties": {
            "originalType": recipe_type,
            "reviewRequired": True,
        },
        "policy": {
            "timeout": "00:30:00",
            "retry": 0,
        },
    }


# ── Trigger generation ────────────────────────────────────────

TRIGGER_TYPE_MAP = {
    "temporal": "ScheduleTrigger",
    "dataset_change": "TumblingWindowTrigger",
    "manual": "Manual",
    "api": "Manual",
    "sql_query": "ScheduleTrigger",
    "custom_python": "Manual",
}


def _convert_single_trigger(trigger: dict) -> dict | None:
    """Convert a single Dataiku trigger to Fabric Pipeline trigger format.

    Supports: temporal, dataset_change, manual, api, sql_query, custom_python.
    """
    trigger_type = trigger.get("type", "").lower()
    fabric_type = TRIGGER_TYPE_MAP.get(trigger_type)

    if not fabric_type:
        return {
            "type": "Manual",
            "name": f"unknown_{trigger_type}",
            "properties": {
                "originalType": trigger_type,
                "reviewRequired": True,
            },
        }

    if trigger_type == "temporal":
        frequency = trigger.get("frequency", "Day")
        interval = trigger.get("interval", 1)
        start_time = trigger.get("startTime", "")

        # Map Dataiku frequency to Fabric recurrence
        freq_map = {
            "minutely": "Minute",
            "hourly": "Hour",
            "daily": "Day",
            "weekly": "Week",
            "monthly": "Month",
        }
        fabric_freq = freq_map.get(frequency.lower(), frequency)

        result: dict[str, Any] = {
            "type": "ScheduleTrigger",
            "name": f"schedule_{fabric_freq.lower()}",
            "properties": {
                "recurrence": {
                    "frequency": fabric_freq,
                    "interval": interval,
                },
            },
        }
        if start_time:
            result["properties"]["recurrence"]["startTime"] = start_time
        return result

    if trigger_type == "dataset_change":
        dataset = trigger.get("dataset", "")
        return {
            "type": "TumblingWindowTrigger",
            "name": f"on_change_{dataset}",
            "properties": {
                "dataset": dataset,
                "notes": "Monitor data changes — use Fabric event-based trigger or scheduled check",
            },
        }

    if trigger_type in ("manual", "api"):
        return {
            "type": "Manual",
            "name": f"{trigger_type}_trigger",
            "properties": {
                "description": f"Originally a {trigger_type} trigger in Dataiku",
            },
        }

    if trigger_type == "sql_query":
        return {
            "type": "ScheduleTrigger",
            "name": "sql_query_check",
            "properties": {
                "recurrence": {
                    "frequency": trigger.get("frequency", "Hour"),
                    "interval": trigger.get("interval", 1),
                },
                "notes": "SQL query trigger — implement as scheduled pipeline with SQL check",
                "originalQuery": trigger.get("query", ""),
            },
        }

    if trigger_type == "custom_python":
        return {
            "type": "Manual",
            "name": "custom_python_trigger",
            "properties": {
                "reviewRequired": True,
                "notes": "Custom Python trigger requires manual conversion",
            },
        }

    return None


def convert_scenario_triggers(scenario_data: dict) -> dict:
    """Convert all triggers from a Dataiku scenario.

    Returns a dict with:
    - ``triggers``: list of converted Fabric triggers
    - ``steps``: list of scenario step references
    - ``review_flags``: any items needing manual review
    """
    triggers = scenario_data.get("triggers", [])
    steps = scenario_data.get("steps", [])
    review_flags: list[str] = []

    converted_triggers: list[dict] = []
    for trigger in triggers:
        result = _convert_single_trigger(trigger)
        if result:
            converted_triggers.append(result)
            if result.get("properties", {}).get("reviewRequired"):
                review_flags.append(
                    f"Trigger '{result.get('name', 'unknown')}' needs manual review"
                )

    # Convert scenario steps to pipeline activity references
    step_refs: list[dict] = []
    for step in steps:
        step_refs.append({
            "name": step.get("name", "unknown_step"),
            "type": step.get("type", "unknown"),
            "target": step.get("target", ""),
        })

    return {
        "triggers": converted_triggers,
        "steps": step_refs,
        "review_flags": review_flags,
    }


# ── Pipeline JSON generation ─────────────────────────────────

def generate_pipeline_json(
    pipeline_name: str,
    flow_data: dict,
    recipe_metadata: dict[str, dict] | None = None,
    converted_assets: dict[str, dict] | None = None,
) -> dict:
    """Generate a Fabric Data Pipeline JSON definition from a Dataiku flow.

    Args:
        pipeline_name: Name for the output pipeline.
        flow_data: Dataiku flow data (graph with items + edges).
        recipe_metadata: Metadata for each recipe (by name).
        converted_assets: Target info for each converted recipe (by name).

    Returns:
        A dict conforming to the Fabric Data Pipeline JSON schema.
    """
    recipe_metadata = recipe_metadata or {}
    converted_assets = converted_assets or {}
    graph = build_flow_dag(flow_data)
    execution_order = get_execution_order(graph)

    activities: list[dict] = []
    review_flags: list[str] = []

    for node_id in execution_order:
        node_data = graph.nodes.get(node_id, {})
        recipe_name = node_data.get("name", node_id)
        recipe_type = node_data.get("type", "sql")

        # Find predecessor activities for dependency chain
        predecessors = list(graph.predecessors(node_id))
        depends_on = [
            f"run_{graph.nodes.get(p, {}).get('name', p)}"
            for p in predecessors
        ]

        # Look up converted asset info for references
        converted_asset = converted_assets.get(recipe_name)

        activity = _recipe_to_activity(recipe_name, recipe_type, depends_on, converted_asset)
        activities.append(activity)

        if activity["type"] == "Placeholder":
            review_flags.append(f"Recipe '{recipe_name}' (type: {recipe_type}) needs manual conversion")

    pipeline: dict[str, Any] = {
        "name": pipeline_name,
        "properties": {
            "description": f"Migrated from Dataiku flow — auto-generated by DataikuToFabric toolkit",
            "activities": activities,
            "parameters": {
                "project_key": {
                    "type": "string",
                    "defaultValue": "",
                },
            },
            "annotations": [
                "Migrated from Dataiku flow",
                f"Activity count: {len(activities)}",
            ],
        },
    }

    if review_flags:
        pipeline["_review_flags"] = review_flags

    return pipeline


# ── Agent ─────────────────────────────────────────────────────

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
        review_flags: list[str] = []

        # Build a lookup of converted recipe assets for activity references
        converted_assets: dict[str, dict] = {}
        for recipe_asset in (
            registry.get_by_type(AssetType.RECIPE_SQL)
            + registry.get_by_type(AssetType.RECIPE_PYTHON)
            + registry.get_by_type(AssetType.RECIPE_VISUAL)
        ):
            if recipe_asset.target_fabric_asset:
                converted_assets[recipe_asset.name] = recipe_asset.target_fabric_asset

        # Convert flows to pipelines
        for asset in flow_assets:
            registry.update_state(asset.id, MigrationState.CONVERTING)
            try:
                pipeline_json = generate_pipeline_json(
                    pipeline_name=f"{asset.source_project}_pipeline",
                    flow_data=asset.metadata,
                    converted_assets=converted_assets,
                )

                out_file = output_dir / f"{asset.name}.json"
                out_file.write_text(json.dumps(pipeline_json, indent=2), encoding="utf-8")

                # Collect review flags from pipeline generation
                pipeline_flags = pipeline_json.get("_review_flags", [])
                review_flags.extend(pipeline_flags)
                for flag in pipeline_flags:
                    registry.add_review_flag(asset.id, flag)

                registry.set_target(asset.id, {
                    "type": "data_pipeline",
                    "path": str(out_file),
                    "activity_count": len(pipeline_json["properties"]["activities"]),
                    "has_review_flags": len(pipeline_flags) > 0,
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
                trigger_result = convert_scenario_triggers(asset.metadata)

                out_file = output_dir / f"{asset.name}_trigger.json"
                out_file.write_text(json.dumps(trigger_result, indent=2), encoding="utf-8")

                # Collect review flags from trigger conversion
                trigger_flags = trigger_result.get("review_flags", [])
                review_flags.extend(trigger_flags)
                for flag in trigger_flags:
                    registry.add_review_flag(asset.id, flag)

                registry.set_target(asset.id, {
                    "type": "pipeline_trigger",
                    "path": str(out_file),
                    "trigger_count": len(trigger_result["triggers"]),
                    "step_count": len(trigger_result.get("steps", [])),
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
            review_flags=review_flags,
            errors=errors,
        )

    async def validate(self, context: Any) -> ValidationResult:
        registry = context.registry
        flow_assets = registry.get_by_type(AssetType.FLOW)
        scenario_assets = registry.get_by_type(AssetType.SCENARIO)
        converted_flows = [a for a in flow_assets if a.state == MigrationState.CONVERTED]
        converted_scenarios = [a for a in scenario_assets if a.state == MigrationState.CONVERTED]

        checks_run = 0
        failures: list[str] = []

        for asset in converted_flows:
            target = asset.target_fabric_asset or {}
            pipeline_path = target.get("path")

            # Check file existence
            checks_run += 1
            if not pipeline_path or not Path(pipeline_path).exists():
                failures.append(f"{asset.name}: Pipeline JSON not found")
                continue

            # Check valid JSON with required structure
            checks_run += 1
            try:
                content = json.loads(Path(pipeline_path).read_text(encoding="utf-8"))
                if "properties" not in content:
                    failures.append(f"{asset.name}: Missing 'properties' key")
                    continue
                if "activities" not in content["properties"]:
                    failures.append(f"{asset.name}: Missing 'activities' in properties")
                    continue
            except json.JSONDecodeError as e:
                failures.append(f"{asset.name}: Invalid JSON — {e}")
                continue

            # Check activity count matches
            checks_run += 1
            actual_count = len(content["properties"]["activities"])
            expected_count = target.get("activity_count", 0)
            if actual_count != expected_count:
                failures.append(
                    f"{asset.name}: Activity count mismatch — expected {expected_count}, got {actual_count}"
                )

            # Check each activity has a name and type
            checks_run += 1
            for act in content["properties"]["activities"]:
                if not act.get("name"):
                    failures.append(f"{asset.name}: Activity missing 'name'")
                if not act.get("type"):
                    failures.append(f"{asset.name}: Activity missing 'type'")

        for asset in converted_scenarios:
            target = asset.target_fabric_asset or {}
            trigger_path = target.get("path")

            checks_run += 1
            if not trigger_path or not Path(trigger_path).exists():
                failures.append(f"{asset.name}: Trigger JSON not found")
                continue

            checks_run += 1
            try:
                content = json.loads(Path(trigger_path).read_text(encoding="utf-8"))
                if "triggers" not in content:
                    failures.append(f"{asset.name}: Missing 'triggers' key in trigger config")
            except json.JSONDecodeError as e:
                failures.append(f"{asset.name}: Invalid JSON — {e}")

        return ValidationResult(
            passed=len(failures) == 0,
            checks_run=checks_run,
            checks_passed=checks_run - len(failures),
            checks_failed=len(failures),
            failures=failures,
        )
