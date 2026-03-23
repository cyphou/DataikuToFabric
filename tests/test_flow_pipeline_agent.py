"""Tests for the flow → pipeline agent and orchestrator DAG — Phase 6."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import networkx as nx
import pytest

from src.agents.flow_pipeline_agent import (
    FlowPipelineAgent,
    TRIGGER_TYPE_MAP,
    _convert_single_trigger,
    _recipe_to_activity,
    build_flow_dag,
    convert_scenario_triggers,
    generate_pipeline_json,
    get_execution_order,
)
from src.agents.base_agent import AgentStatus
from src.core.orchestrator import (
    DEFAULT_AGENT_GRAPH,
    build_agent_dag,
    get_execution_waves,
)
from src.core.registry import AssetRegistry
from src.models.asset import Asset, AssetType, MigrationState


# ── Helpers ───────────────────────────────────────────────────

@dataclass
class StubFabricConfig:
    workspace_id: str = "00000000-0000-0000-0000-000000000000"


@dataclass
class StubMigrationConfig:
    output_dir: str = "./output"
    default_storage: str = "lakehouse"
    parallel_agents: bool = False
    max_concurrent_agents: int = 4
    fail_fast: bool = False


@dataclass
class StubConfig:
    fabric: Any = None
    migration: Any = None

    def __post_init__(self):
        self.fabric = self.fabric or StubFabricConfig()
        self.migration = self.migration or StubMigrationConfig()


@dataclass
class StubContext:
    registry: Any = None
    config: Any = None


def _make_flow_data(items: dict[str, dict], edges: list[dict]) -> dict:
    """Build a Dataiku flow data structure."""
    return {"graph": {"items": items, "edges": edges}}


def _make_flow_asset(name: str, flow_data: dict) -> Asset:
    return Asset(
        id=f"flow_{name}",
        type=AssetType.FLOW,
        name=name,
        source_project="TEST",
        state=MigrationState.DISCOVERED,
        metadata=flow_data,
    )


def _make_scenario_asset(name: str, triggers: list[dict], steps: list[dict] | None = None) -> Asset:
    return Asset(
        id=f"scenario_{name}",
        type=AssetType.SCENARIO,
        name=name,
        source_project="TEST",
        state=MigrationState.DISCOVERED,
        metadata={"triggers": triggers, "steps": steps or []},
    )


SIMPLE_FLOW = _make_flow_data(
    items={
        "r1": {"name": "extract_data", "type": "sql"},
        "r2": {"name": "transform_data", "type": "python"},
        "r3": {"name": "load_data", "type": "sql"},
    },
    edges=[
        {"from": "r1", "to": "r2"},
        {"from": "r2", "to": "r3"},
    ],
)

PARALLEL_FLOW = _make_flow_data(
    items={
        "r1": {"name": "source_a", "type": "sql"},
        "r2": {"name": "source_b", "type": "python"},
        "r3": {"name": "merge_data", "type": "join"},
    },
    edges=[
        {"from": "r1", "to": "r3"},
        {"from": "r2", "to": "r3"},
    ],
)


# ═══════════════════════════════════════════════════════════════
# 1. DAG construction tests
# ═══════════════════════════════════════════════════════════════

class TestBuildFlowDAG:
    """Test build_flow_dag() from Dataiku flow data."""

    def test_simple_linear_flow(self):
        graph = build_flow_dag(SIMPLE_FLOW)
        assert len(graph.nodes) == 3
        assert len(graph.edges) == 2
        assert graph.has_edge("r1", "r2")
        assert graph.has_edge("r2", "r3")

    def test_parallel_flow(self):
        graph = build_flow_dag(PARALLEL_FLOW)
        assert len(graph.nodes) == 3
        assert graph.has_edge("r1", "r3")
        assert graph.has_edge("r2", "r3")
        assert not graph.has_edge("r1", "r2")

    def test_empty_flow(self):
        graph = build_flow_dag({"graph": {"items": {}, "edges": []}})
        assert len(graph.nodes) == 0

    def test_disconnected_nodes(self):
        flow = _make_flow_data(
            items={"a": {"name": "a"}, "b": {"name": "b"}},
            edges=[],
        )
        graph = build_flow_dag(flow)
        assert len(graph.nodes) == 2
        assert len(graph.edges) == 0

    def test_node_attributes_preserved(self):
        graph = build_flow_dag(SIMPLE_FLOW)
        assert graph.nodes["r1"]["name"] == "extract_data"
        assert graph.nodes["r2"]["type"] == "python"

    def test_missing_graph_key(self):
        graph = build_flow_dag({})
        assert len(graph.nodes) == 0


class TestExecutionOrder:
    """Test get_execution_order() topological sort."""

    def test_linear_order(self):
        graph = build_flow_dag(SIMPLE_FLOW)
        order = get_execution_order(graph)
        assert order.index("r1") < order.index("r2")
        assert order.index("r2") < order.index("r3")

    def test_parallel_predecessors_before_merge(self):
        graph = build_flow_dag(PARALLEL_FLOW)
        order = get_execution_order(graph)
        assert order.index("r3") > order.index("r1")
        assert order.index("r3") > order.index("r2")

    def test_cycle_falls_back(self):
        """Cyclic graph returns nodes without crashing."""
        g = nx.DiGraph()
        g.add_edges_from([("a", "b"), ("b", "c"), ("c", "a")])
        order = get_execution_order(g)
        assert set(order) == {"a", "b", "c"}


# ═══════════════════════════════════════════════════════════════
# 2. Activity generation tests
# ═══════════════════════════════════════════════════════════════

class TestRecipeToActivity:
    """Test _recipe_to_activity() for different recipe types."""

    def test_python_recipe(self):
        act = _recipe_to_activity("my_script", "python", [])
        assert act["type"] == "SynapseNotebook"
        assert act["name"] == "run_my_script"
        assert "notebook" in act["typeProperties"]

    def test_pyspark_recipe(self):
        act = _recipe_to_activity("spark_job", "pyspark", [])
        assert act["type"] == "SynapseNotebook"

    def test_r_recipe(self):
        act = _recipe_to_activity("r_job", "r", [])
        assert act["type"] == "SynapseNotebook"

    def test_sql_recipe(self):
        act = _recipe_to_activity("query1", "sql", [])
        assert act["type"] == "Script"
        assert act["typeProperties"]["scriptType"] == "SQL"

    def test_visual_recipe_types(self):
        for vtype in ("join", "group", "filter", "window", "sort", "topn", "pivot", "prepare"):
            act = _recipe_to_activity(f"vis_{vtype}", vtype, [])
            assert act["type"] == "Script", f"{vtype} should map to Script"

    def test_unknown_recipe_type(self):
        act = _recipe_to_activity("exotic", "spark_streaming", [])
        assert act["type"] == "Placeholder"
        assert act["typeProperties"]["reviewRequired"] is True

    def test_dependencies_mapped(self):
        act = _recipe_to_activity("step2", "sql", ["run_step1", "run_step0"])
        assert len(act["dependsOn"]) == 2
        assert act["dependsOn"][0]["activity"] == "run_step1"
        assert act["dependsOn"][0]["dependencyConditions"] == ["Succeeded"]

    def test_no_dependencies(self):
        act = _recipe_to_activity("root", "sql", [])
        assert act["dependsOn"] == []

    def test_activity_has_policy(self):
        act = _recipe_to_activity("job", "python", [])
        assert "policy" in act
        assert "timeout" in act["policy"]

    def test_converted_asset_reference(self):
        asset = {"notebook_name": "custom_notebook"}
        act = _recipe_to_activity("recipe", "python", [], converted_asset=asset)
        assert "custom_notebook_notebook" in act["typeProperties"]["notebook"]["referenceName"]

    def test_converted_asset_sql_path(self):
        asset = {"path": "output/sql/custom.sql"}
        act = _recipe_to_activity("recipe", "sql", [], converted_asset=asset)
        assert act["typeProperties"]["scriptPath"] == "output/sql/custom.sql"


# ═══════════════════════════════════════════════════════════════
# 3. Trigger conversion tests
# ═══════════════════════════════════════════════════════════════

class TestTriggerConversion:
    """Test _convert_single_trigger() for all trigger types."""

    def test_temporal_trigger(self):
        trigger = {"type": "temporal", "frequency": "Daily", "interval": 2}
        result = _convert_single_trigger(trigger)
        assert result["type"] == "ScheduleTrigger"
        assert result["properties"]["recurrence"]["frequency"] == "Day"
        assert result["properties"]["recurrence"]["interval"] == 2

    def test_temporal_with_start_time(self):
        trigger = {"type": "temporal", "frequency": "Hourly", "interval": 1,
                    "startTime": "2026-01-01T00:00:00Z"}
        result = _convert_single_trigger(trigger)
        assert result["properties"]["recurrence"]["startTime"] == "2026-01-01T00:00:00Z"

    def test_temporal_weekly(self):
        trigger = {"type": "temporal", "frequency": "Weekly", "interval": 1}
        result = _convert_single_trigger(trigger)
        assert result["properties"]["recurrence"]["frequency"] == "Week"

    def test_dataset_change_trigger(self):
        trigger = {"type": "dataset_change", "dataset": "sales_data"}
        result = _convert_single_trigger(trigger)
        assert result["type"] == "TumblingWindowTrigger"
        assert result["properties"]["dataset"] == "sales_data"

    def test_manual_trigger(self):
        result = _convert_single_trigger({"type": "manual"})
        assert result["type"] == "Manual"

    def test_api_trigger(self):
        result = _convert_single_trigger({"type": "api"})
        assert result["type"] == "Manual"

    def test_sql_query_trigger(self):
        trigger = {"type": "sql_query", "query": "SELECT 1", "frequency": "Minute", "interval": 5}
        result = _convert_single_trigger(trigger)
        assert result["type"] == "ScheduleTrigger"
        assert result["properties"]["originalQuery"] == "SELECT 1"

    def test_custom_python_trigger(self):
        result = _convert_single_trigger({"type": "custom_python"})
        assert result["type"] == "Manual"
        assert result["properties"]["reviewRequired"] is True

    def test_unknown_trigger(self):
        result = _convert_single_trigger({"type": "exotic_trigger"})
        assert result["type"] == "Manual"
        assert result["properties"]["reviewRequired"] is True

    def test_all_types_in_map(self):
        expected = {"temporal", "dataset_change", "manual", "api", "sql_query", "custom_python"}
        assert expected == set(TRIGGER_TYPE_MAP.keys())


class TestConvertScenarioTriggers:
    """Test convert_scenario_triggers() with full scenarios."""

    def test_multiple_triggers(self):
        scenario = {
            "triggers": [
                {"type": "temporal", "frequency": "Daily", "interval": 1},
                {"type": "dataset_change", "dataset": "orders"},
            ],
            "steps": [
                {"name": "build_dataset", "type": "build", "target": "output_ds"},
            ],
        }
        result = convert_scenario_triggers(scenario)
        assert len(result["triggers"]) == 2
        assert len(result["steps"]) == 1
        assert result["steps"][0]["name"] == "build_dataset"

    def test_empty_scenario(self):
        result = convert_scenario_triggers({"triggers": [], "steps": []})
        assert result["triggers"] == []
        assert result["steps"] == []

    def test_review_flags_collected(self):
        scenario = {
            "triggers": [{"type": "custom_python"}],
            "steps": [],
        }
        result = convert_scenario_triggers(scenario)
        assert len(result["review_flags"]) == 1
        assert "manual review" in result["review_flags"][0]


# ═══════════════════════════════════════════════════════════════
# 4. Pipeline JSON generation tests
# ═══════════════════════════════════════════════════════════════

class TestGeneratePipelineJson:
    """Test generate_pipeline_json() end-to-end."""

    def test_simple_pipeline(self):
        pipeline = generate_pipeline_json("test_pipeline", SIMPLE_FLOW)
        assert pipeline["name"] == "test_pipeline"
        activities = pipeline["properties"]["activities"]
        assert len(activities) == 3

    def test_activity_order_respects_deps(self):
        pipeline = generate_pipeline_json("ordered", SIMPLE_FLOW)
        names = [a["name"] for a in pipeline["properties"]["activities"]]
        assert names.index("run_extract_data") < names.index("run_transform_data")
        assert names.index("run_transform_data") < names.index("run_load_data")

    def test_activity_types_match_recipe_types(self):
        pipeline = generate_pipeline_json("typed", SIMPLE_FLOW)
        activities = {a["name"]: a for a in pipeline["properties"]["activities"]}
        assert activities["run_extract_data"]["type"] == "Script"
        assert activities["run_transform_data"]["type"] == "SynapseNotebook"
        assert activities["run_load_data"]["type"] == "Script"

    def test_dependencies_in_activities(self):
        pipeline = generate_pipeline_json("deps", SIMPLE_FLOW)
        activities = {a["name"]: a for a in pipeline["properties"]["activities"]}
        assert activities["run_extract_data"]["dependsOn"] == []
        assert len(activities["run_transform_data"]["dependsOn"]) == 1
        assert activities["run_transform_data"]["dependsOn"][0]["activity"] == "run_extract_data"

    def test_parallel_flow_dependencies(self):
        pipeline = generate_pipeline_json("parallel", PARALLEL_FLOW)
        activities = {a["name"]: a for a in pipeline["properties"]["activities"]}
        merge = activities["run_merge_data"]
        dep_names = [d["activity"] for d in merge["dependsOn"]]
        assert "run_source_a" in dep_names
        assert "run_source_b" in dep_names

    def test_empty_flow(self):
        pipeline = generate_pipeline_json("empty", {"graph": {"items": {}, "edges": []}})
        assert len(pipeline["properties"]["activities"]) == 0

    def test_pipeline_has_annotations(self):
        pipeline = generate_pipeline_json("annotated", SIMPLE_FLOW)
        assert "annotations" in pipeline["properties"]
        assert any("Dataiku" in a for a in pipeline["properties"]["annotations"])

    def test_pipeline_has_parameters(self):
        pipeline = generate_pipeline_json("params", SIMPLE_FLOW)
        assert "parameters" in pipeline["properties"]
        assert "project_key" in pipeline["properties"]["parameters"]

    def test_unknown_type_generates_review_flags(self):
        flow = _make_flow_data(
            items={"r1": {"name": "exotic_job", "type": "spark_streaming"}},
            edges=[],
        )
        pipeline = generate_pipeline_json("flagged", flow)
        assert "_review_flags" in pipeline
        assert len(pipeline["_review_flags"]) == 1

    def test_converted_assets_referenced(self):
        converted = {"extract_data": {"path": "output/sql/extract_data.sql"}}
        pipeline = generate_pipeline_json("refs", SIMPLE_FLOW, converted_assets=converted)
        activities = {a["name"]: a for a in pipeline["properties"]["activities"]}
        assert activities["run_extract_data"]["typeProperties"]["scriptPath"] == "output/sql/extract_data.sql"


# ═══════════════════════════════════════════════════════════════
# 5. Agent execute tests
# ═══════════════════════════════════════════════════════════════

class TestFlowPipelineAgentExecute:
    """Test FlowPipelineAgent.execute()."""

    @pytest.fixture
    def agent(self):
        return FlowPipelineAgent()

    @pytest.fixture
    def ctx(self, tmp_path):
        reg = AssetRegistry("TEST", registry_path=tmp_path / "reg.json")
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        return StubContext(registry=reg, config=cfg)

    def test_converts_flow(self, agent, ctx):
        ctx.registry.add_asset(_make_flow_asset("main_flow", SIMPLE_FLOW))
        result = asyncio.run(agent.execute(ctx))

        assert result.status == AgentStatus.COMPLETED
        assert result.assets_processed == 1
        assert result.assets_converted == 1

    def test_creates_pipeline_json(self, agent, ctx):
        ctx.registry.add_asset(_make_flow_asset("main_flow", SIMPLE_FLOW))
        asyncio.run(agent.execute(ctx))

        out = Path(ctx.config.migration.output_dir) / "pipelines" / "main_flow.json"
        assert out.exists()
        data = json.loads(out.read_text())
        assert len(data["properties"]["activities"]) == 3

    def test_converts_scenario(self, agent, ctx):
        ctx.registry.add_asset(_make_scenario_asset(
            "daily_run",
            triggers=[{"type": "temporal", "frequency": "Daily", "interval": 1}],
            steps=[{"name": "build_all", "type": "build", "target": "output"}],
        ))
        result = asyncio.run(agent.execute(ctx))

        assert result.assets_converted == 1

    def test_scenario_creates_trigger_file(self, agent, ctx):
        ctx.registry.add_asset(_make_scenario_asset("nightly", [{"type": "temporal", "frequency": "Daily"}]))
        asyncio.run(agent.execute(ctx))

        out = Path(ctx.config.migration.output_dir) / "pipelines" / "nightly_trigger.json"
        assert out.exists()
        data = json.loads(out.read_text())
        assert len(data["triggers"]) == 1

    def test_flow_and_scenarios_together(self, agent, ctx):
        ctx.registry.add_asset(_make_flow_asset("flow1", SIMPLE_FLOW))
        ctx.registry.add_asset(_make_scenario_asset("sched1", [{"type": "temporal"}]))
        result = asyncio.run(agent.execute(ctx))

        assert result.assets_processed == 2
        assert result.assets_converted == 2

    def test_sets_registry_target(self, agent, ctx):
        ctx.registry.add_asset(_make_flow_asset("f1", SIMPLE_FLOW))
        asyncio.run(agent.execute(ctx))

        asset = ctx.registry.get_asset("flow_f1")
        assert asset.state == MigrationState.CONVERTED
        assert asset.target_fabric_asset["type"] == "data_pipeline"
        assert asset.target_fabric_asset["activity_count"] == 3

    def test_scenario_target_has_trigger_count(self, agent, ctx):
        ctx.registry.add_asset(_make_scenario_asset("s1", [
            {"type": "temporal"},
            {"type": "manual"},
        ]))
        asyncio.run(agent.execute(ctx))

        asset = ctx.registry.get_asset("scenario_s1")
        assert asset.target_fabric_asset["trigger_count"] == 2


# ═══════════════════════════════════════════════════════════════
# 6. Agent validate tests
# ═══════════════════════════════════════════════════════════════

class TestFlowPipelineAgentValidate:
    """Test FlowPipelineAgent.validate()."""

    @pytest.fixture
    def agent(self):
        return FlowPipelineAgent()

    @pytest.fixture
    def ctx(self, tmp_path):
        reg = AssetRegistry("TEST", registry_path=tmp_path / "reg.json")
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        return StubContext(registry=reg, config=cfg)

    def test_passes_after_execute(self, agent, ctx):
        ctx.registry.add_asset(_make_flow_asset("valid", SIMPLE_FLOW))
        asyncio.run(agent.execute(ctx))
        result = asyncio.run(agent.validate(ctx))

        assert result.passed is True
        assert result.checks_run >= 3

    def test_fails_missing_pipeline(self, agent, ctx):
        asset = _make_flow_asset("missing", SIMPLE_FLOW)
        ctx.registry.add_asset(asset)
        ctx.registry.update_state("flow_missing", MigrationState.CONVERTED)
        ctx.registry.set_target("flow_missing", {"path": "/nonexistent.json", "activity_count": 0})

        result = asyncio.run(agent.validate(ctx))
        assert result.passed is False

    def test_validates_scenario_triggers(self, agent, ctx):
        ctx.registry.add_asset(_make_scenario_asset("s1", [{"type": "temporal"}]))
        asyncio.run(agent.execute(ctx))
        result = asyncio.run(agent.validate(ctx))

        assert result.passed is True

    def test_no_assets_passes(self, agent, ctx):
        result = asyncio.run(agent.validate(ctx))
        assert result.passed is True
        assert result.checks_run == 0


# ═══════════════════════════════════════════════════════════════
# 7. Orchestrator DAG tests
# ═══════════════════════════════════════════════════════════════

class TestBuildAgentDAG:
    """Test build_agent_dag() and get_execution_waves()."""

    def test_default_graph_structure(self):
        all_names = [name for name, _ in DEFAULT_AGENT_GRAPH]
        dag = build_agent_dag(all_names)
        assert "discovery" in dag.nodes
        assert "validator" in dag.nodes
        assert dag.has_edge("discovery", "sql_converter")

    def test_filters_to_available_agents(self):
        dag = build_agent_dag(["discovery", "sql_converter"])
        assert len(dag.nodes) == 2
        assert "validator" not in dag.nodes

    def test_drops_missing_dependencies(self):
        dag = build_agent_dag(["sql_converter"])
        # discovery not in list, so no edge to sql_converter
        assert len(list(dag.predecessors("sql_converter"))) == 0

    def test_discovery_has_no_predecessors(self):
        all_names = [name for name, _ in DEFAULT_AGENT_GRAPH]
        dag = build_agent_dag(all_names)
        assert len(list(dag.predecessors("discovery"))) == 0

    def test_validator_depends_on_builder(self):
        all_names = [name for name, _ in DEFAULT_AGENT_GRAPH]
        dag = build_agent_dag(all_names)
        assert dag.has_edge("pipeline_builder", "validator")


class TestExecutionWaves:
    """Test get_execution_waves() parallel grouping."""

    def test_linear_dag_gives_sequential_waves(self):
        dag = nx.DiGraph()
        dag.add_edges_from([("a", "b"), ("b", "c")])
        waves = get_execution_waves(dag)
        assert waves == [["a"], ["b"], ["c"]]

    def test_parallel_nodes_in_same_wave(self):
        dag = nx.DiGraph()
        dag.add_edges_from([("root", "a"), ("root", "b"), ("a", "end"), ("b", "end")])
        waves = get_execution_waves(dag)
        assert waves[0] == ["root"]
        assert set(waves[1]) == {"a", "b"}
        assert waves[2] == ["end"]

    def test_full_agent_waves(self):
        all_names = [name for name, _ in DEFAULT_AGENT_GRAPH]
        dag = build_agent_dag(all_names)
        waves = get_execution_waves(dag)

        # Wave 1: discovery (no deps)
        assert waves[0] == ["discovery"]
        # Wave 2: all agents that depend only on discovery
        wave2_set = set(waves[1])
        assert "connection_mapper" in wave2_set
        assert "dataset_migrator" in wave2_set
        assert "sql_converter" in wave2_set

    def test_empty_dag(self):
        dag = nx.DiGraph()
        waves = get_execution_waves(dag)
        assert waves == []

    def test_single_node(self):
        dag = nx.DiGraph()
        dag.add_node("only")
        waves = get_execution_waves(dag)
        assert waves == [["only"]]

    def test_custom_graph_spec(self):
        spec = [("a", []), ("b", ["a"]), ("c", ["a"]), ("d", ["b", "c"])]
        dag = build_agent_dag(["a", "b", "c", "d"], graph_spec=spec)
        waves = get_execution_waves(dag)
        assert waves[0] == ["a"]
        assert set(waves[1]) == {"b", "c"}
        assert waves[2] == ["d"]
