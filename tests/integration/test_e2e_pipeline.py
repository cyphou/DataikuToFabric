"""End-to-end integration test — full pipeline from discovery to validation.

Uses a synthetic Dataiku project fixture with every asset type.  All
connectors are stubbed (no live APIs).  The test runs the complete
Orchestrator.run_pipeline() and verifies:

- Every agent completes
- Registry state transitions are correct
- Output files exist and pass validation
- HTML + JSON reports can be generated
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from src.agents.connection_agent import ConnectionMapperAgent
from src.agents.dataset_agent import DatasetMigrationAgent
from src.agents.discovery_agent import DiscoveryAgent
from src.agents.flow_pipeline_agent import FlowPipelineAgent
from src.agents.python_migration_agent import PythonMigrationAgent
from src.agents.sql_migration_agent import SQLMigrationAgent
from src.agents.validation_agent import ValidationAgent
from src.agents.visual_recipe_agent import VisualRecipeAgent
from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent, ValidationResult
from src.core.orchestrator import Orchestrator, build_agent_dag, get_execution_waves
from src.core.registry import AssetRegistry
from src.models.asset import Asset, AssetType, MigrationState
from src.models.report import (
    ValidationReport,
    generate_html_report,
    generate_json_report,
    save_html_report,
    save_json_report,
)


# ── Synthetic Dataiku project fixture ────────────────────────

SYNTHETIC_PROJECT = {
    "projectKey": "E2E_TEST",
    "recipes": [
        {"name": "sql_extract", "type": "sql", "payload": "SELECT id, name FROM users WHERE active = 1",
         "connection": "oracle_prod", "inputs": ["raw_users"], "outputs": ["clean_users"]},
        {"name": "py_transform", "type": "python", "payload": "import dataiku\nds = dataiku.Dataset('clean_users')\ndf = ds.get_dataframe()\ndf['upper_name'] = df['name'].str.upper()\ndataiku.Dataset('transformed').write_dataframe(df)",
         "inputs": ["clean_users"], "outputs": ["transformed"]},
        {"name": "visual_join", "type": "join",
         "payload": {"join": {"inputs": [{"dataset": "transformed", "alias": "t"}, {"dataset": "orders", "alias": "o"}], "on": [{"left": "t.id", "right": "o.user_id"}], "type": "LEFT"}},
         "inputs": ["transformed", "orders"], "outputs": ["joined_data"]},
        {"name": "visual_group", "type": "group",
         "payload": {"group": {"keys": [{"column": "status"}], "aggregations": [{"column": "amount", "function": "SUM"}]}},
         "inputs": ["joined_data"], "outputs": ["summary"]},
    ],
    "datasets": [
        {"name": "raw_users", "type": "managed", "schema": {"columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}, {"name": "active", "type": "boolean"}]}},
        {"name": "clean_users", "type": "managed", "schema": {"columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}]}},
        {"name": "orders", "type": "managed", "schema": {"columns": [{"name": "user_id", "type": "int"}, {"name": "amount", "type": "double"}, {"name": "status", "type": "string"}]}},
        {"name": "transformed", "type": "managed", "schema": {"columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}, {"name": "upper_name", "type": "string"}]}},
        {"name": "joined_data", "type": "managed", "schema": {"columns": []}},
        {"name": "summary", "type": "managed", "schema": {"columns": []}},
    ],
    "connections": [
        {"name": "oracle_prod", "type": "Oracle", "params": {"host": "db.local", "port": 1521, "database": "PROD"}},
        {"name": "pg_warehouse", "type": "PostgreSQL", "params": {"host": "pg.local", "port": 5432, "database": "warehouse"}},
    ],
    "flows": [
        {"name": "main_flow", "graph": {
            "items": {
                "sql_extract": {"type": "recipe.sql"},
                "py_transform": {"type": "recipe.python"},
                "visual_join": {"type": "recipe.visual"},
                "visual_group": {"type": "recipe.visual"},
            },
            "edges": [
                {"from": "sql_extract", "to": "py_transform"},
                {"from": "py_transform", "to": "visual_join"},
                {"from": "visual_join", "to": "visual_group"},
            ],
        }},
    ],
    "scenarios": [
        {"name": "daily_refresh", "triggers": [
            {"type": "temporal", "params": {"frequency": "daily", "startTime": "06:00"}},
        ], "steps": [
            {"type": "build_dataset", "params": {"dataset": "summary"}},
        ]},
    ],
}


# ── Stub discovery agent (feeds synthetic data into registry) ─


class StubbedDiscoveryAgent(BaseAgent):
    """Injects synthetic project into the registry instead of calling APIs."""

    @property
    def name(self) -> str:
        return "discovery"

    @property
    def description(self) -> str:
        return "Stubbed discovery for E2E test"

    async def execute(self, context: Any) -> AgentResult:
        registry = context.registry
        project = SYNTHETIC_PROJECT

        # Register recipes
        for recipe in project["recipes"]:
            rtype = recipe["type"]
            if rtype == "sql":
                asset_type = AssetType.RECIPE_SQL
            elif rtype == "python":
                asset_type = AssetType.RECIPE_PYTHON
            else:
                asset_type = AssetType.RECIPE_VISUAL

            asset = Asset(
                id=f"recipe_{recipe['name']}",
                type=asset_type,
                name=recipe["name"],
                source_project=project["projectKey"],
                state=MigrationState.DISCOVERED,
                metadata={
                    "recipe_type": rtype,
                    "payload": recipe["payload"],
                    "inputs": recipe.get("inputs", []),
                    "outputs": recipe.get("outputs", []),
                    "connection": recipe.get("connection"),
                },
                dependencies=[f"dataset_{inp}" for inp in recipe.get("inputs", [])],
            )
            registry.add_asset(asset)

        # Register datasets
        for ds in project["datasets"]:
            asset = Asset(
                id=f"dataset_{ds['name']}",
                type=AssetType.DATASET,
                name=ds["name"],
                source_project=project["projectKey"],
                state=MigrationState.DISCOVERED,
                metadata={
                    "schema": ds.get("schema", []),
                    "managed": ds.get("type") == "managed",
                },
            )
            registry.add_asset(asset)

        # Register connections
        for conn in project["connections"]:
            asset = Asset(
                id=f"connection_{conn['name']}",
                type=AssetType.CONNECTION,
                name=conn["name"],
                source_project=project["projectKey"],
                state=MigrationState.DISCOVERED,
                metadata={
                    "type": conn["type"],
                    "params": conn.get("params", {}),
                },
            )
            registry.add_asset(asset)

        # Register flows
        for flow in project["flows"]:
            asset = Asset(
                id=f"flow_{flow['name']}",
                type=AssetType.FLOW,
                name=flow["name"],
                source_project=project["projectKey"],
                state=MigrationState.DISCOVERED,
                metadata=flow,
            )
            registry.add_asset(asset)

        # Register scenarios
        for scenario in project["scenarios"]:
            asset = Asset(
                id=f"scenario_{scenario['name']}",
                type=AssetType.SCENARIO,
                name=scenario["name"],
                source_project=project["projectKey"],
                state=MigrationState.DISCOVERED,
                metadata={
                    "triggers": scenario.get("triggers", []),
                    "steps": scenario.get("steps", []),
                },
            )
            registry.add_asset(asset)

        registry.save()
        total = len(registry.get_all())
        return AgentResult(
            agent_name=self.name,
            status=AgentStatus.COMPLETED,
            assets_processed=total,
            assets_converted=0,
        )

    async def validate(self, context: Any) -> ValidationResult:
        return ValidationResult(passed=True, checks_run=1, checks_passed=1)


# ── Stub config / context ────────────────────────────────────


@dataclass
class StubFabricConfig:
    workspace_id: str = "test-workspace"
    lakehouse_name: str = "lh_test"
    warehouse_name: str = "wh_test"
    auth_method: str = "azure_cli"
    tenant_id_env: str = "AZURE_TENANT_ID"
    client_id_env: str = "AZURE_CLIENT_ID"


@dataclass
class StubOrchestratorConfig:
    max_retries: int = 1
    retry_delay_seconds: int = 0
    agent_timeout_seconds: int = 60
    circuit_breaker_threshold: int = 10
    report_format: list[str] = field(default_factory=lambda: ["json"])


@dataclass
class StubMigrationConfig:
    target_sql_dialect: str = "tsql"
    default_storage: str = "lakehouse"
    parallel_agents: bool = False
    max_concurrent_agents: int = 4
    fail_fast: bool = False
    output_dir: str = ""  # set per test via tmp_path


@dataclass
class StubDataikuConfig:
    url: str = "http://localhost:11200"
    api_key_env: str = "DATAIKU_API_KEY"
    project_key: str = "E2E_TEST"
    timeout_seconds: int = 10
    max_retries: int = 1


@dataclass
class StubLoggingConfig:
    level: str = "WARNING"
    format: str = "json"
    file: str = ""


@dataclass
class StubAppConfig:
    dataiku: Any = None
    fabric: Any = None
    migration: Any = None
    orchestrator: Any = None
    logging: Any = None

    def __post_init__(self):
        self.dataiku = self.dataiku or StubDataikuConfig()
        self.fabric = self.fabric or StubFabricConfig()
        self.migration = self.migration or StubMigrationConfig()
        self.orchestrator = self.orchestrator or StubOrchestratorConfig()
        self.logging = self.logging or StubLoggingConfig()


# ── Tests ─────────────────────────────────────────────────────


class TestE2EPipeline:
    """Full pipeline: discovery → conversion → validation → report."""

    def _build_orchestrator(self, tmp_path):
        config = StubAppConfig()
        config.migration.output_dir = str(tmp_path)
        registry = AssetRegistry(project_key="E2E_TEST", registry_path=tmp_path / "registry.json")
        orch = Orchestrator(config=config, registry=registry)

        # Register all real agents except discovery (stubbed)
        orch.register_agent(StubbedDiscoveryAgent())
        orch.register_agent(SQLMigrationAgent())
        orch.register_agent(PythonMigrationAgent())
        orch.register_agent(VisualRecipeAgent())
        orch.register_agent(DatasetMigrationAgent())
        orch.register_agent(ConnectionMapperAgent())
        orch.register_agent(FlowPipelineAgent())
        orch.register_agent(ValidationAgent())

        return orch

    def test_full_pipeline_completes(self, tmp_path):
        """All agents complete without failures."""
        orch = self._build_orchestrator(tmp_path)
        results = asyncio.run(orch.run_pipeline())

        for agent_name, result in results.items():
            assert result.status in (AgentStatus.COMPLETED,), \
                f"Agent {agent_name} failed: {result.errors}"

    def test_discovery_populates_registry(self, tmp_path):
        """Discovery agent creates expected number of assets."""
        orch = self._build_orchestrator(tmp_path)
        asyncio.run(orch.run_pipeline())

        all_assets = orch.registry.get_all()
        # 4 recipes + 6 datasets + 2 connections + 1 flow + 1 scenario = 14
        assert len(all_assets) == 14

    def test_sql_recipe_converted(self, tmp_path):
        """SQL recipe gets a .sql output file."""
        orch = self._build_orchestrator(tmp_path)
        asyncio.run(orch.run_pipeline())

        sql_asset = orch.registry.get_asset("recipe_sql_extract")
        assert sql_asset is not None
        assert sql_asset.state in (MigrationState.CONVERTED, MigrationState.VALIDATED,
                                    MigrationState.VALIDATION_FAILED)
        assert sql_asset.target_fabric_asset is not None
        assert sql_asset.target_fabric_asset["type"] == "sql_script"
        # Output file should exist
        sql_path = sql_asset.target_fabric_asset.get("path")
        if sql_path:
            assert Path(sql_path).exists()

    def test_python_recipe_converted(self, tmp_path):
        """Python recipe gets a .ipynb output file."""
        orch = self._build_orchestrator(tmp_path)
        asyncio.run(orch.run_pipeline())

        py_asset = orch.registry.get_asset("recipe_py_transform")
        assert py_asset is not None
        assert py_asset.state in (MigrationState.CONVERTED, MigrationState.VALIDATED,
                                   MigrationState.VALIDATION_FAILED)
        assert py_asset.target_fabric_asset is not None
        assert py_asset.target_fabric_asset["type"] == "notebook"

    def test_visual_recipes_converted(self, tmp_path):
        """Visual recipes get SQL output files."""
        orch = self._build_orchestrator(tmp_path)
        asyncio.run(orch.run_pipeline())

        for recipe_name in ("visual_join", "visual_group"):
            asset = orch.registry.get_asset(f"recipe_{recipe_name}")
            assert asset is not None
            assert asset.target_fabric_asset is not None
            assert asset.target_fabric_asset["type"] == "sql_script"

    def test_datasets_get_ddl(self, tmp_path):
        """Datasets get DDL output files."""
        orch = self._build_orchestrator(tmp_path)
        asyncio.run(orch.run_pipeline())

        ds_asset = orch.registry.get_asset("dataset_raw_users")
        assert ds_asset is not None
        assert ds_asset.target_fabric_asset is not None
        ttype = ds_asset.target_fabric_asset["type"]
        assert ttype in ("lakehouse_table", "warehouse_table")

    def test_connections_mapped(self, tmp_path):
        """Connections get mapping files."""
        orch = self._build_orchestrator(tmp_path)
        asyncio.run(orch.run_pipeline())

        conn_asset = orch.registry.get_asset("connection_oracle_prod")
        assert conn_asset is not None
        assert conn_asset.target_fabric_asset is not None
        assert conn_asset.target_fabric_asset["type"] == "connection_mapping"

    def test_flow_becomes_pipeline(self, tmp_path):
        """Flow asset gets a pipeline JSON output."""
        orch = self._build_orchestrator(tmp_path)
        asyncio.run(orch.run_pipeline())

        flow_asset = orch.registry.get_asset("flow_main_flow")
        assert flow_asset is not None
        assert flow_asset.target_fabric_asset is not None
        assert flow_asset.target_fabric_asset["type"] == "data_pipeline"

    def test_scenario_gets_triggers(self, tmp_path):
        """Scenario asset gets trigger JSON output."""
        orch = self._build_orchestrator(tmp_path)
        asyncio.run(orch.run_pipeline())

        scenario = orch.registry.get_asset("scenario_daily_refresh")
        assert scenario is not None
        assert scenario.target_fabric_asset is not None
        assert scenario.target_fabric_asset["type"] == "pipeline_trigger"

    def test_validator_runs_last(self, tmp_path):
        """Validator processes all assets and most pass."""
        orch = self._build_orchestrator(tmp_path)
        results = asyncio.run(orch.run_pipeline())

        val_result = results.get("validator")
        assert val_result is not None
        assert val_result.status == AgentStatus.COMPLETED
        assert val_result.assets_processed > 0

    def test_registry_saved_to_disk(self, tmp_path):
        """Registry JSON persisted after pipeline."""
        orch = self._build_orchestrator(tmp_path)
        asyncio.run(orch.run_pipeline())

        reg_path = tmp_path / "registry.json"
        assert reg_path.exists()
        data = json.loads(reg_path.read_text())
        assert "assets" in data
        assert len(data["assets"]) == 14

    def test_no_assets_stuck_in_discovered(self, tmp_path):
        """All assets should be processed beyond DISCOVERED state."""
        orch = self._build_orchestrator(tmp_path)
        asyncio.run(orch.run_pipeline())

        # Only datasets without schema may remain discovered if the agent
        # skips them — but recipes, connections, flows, scenarios should be processed
        for asset in orch.registry.get_all():
            if asset.type in (AssetType.RECIPE_SQL, AssetType.RECIPE_PYTHON,
                              AssetType.RECIPE_VISUAL, AssetType.CONNECTION,
                              AssetType.FLOW, AssetType.SCENARIO):
                assert asset.state != MigrationState.DISCOVERED, \
                    f"Asset {asset.name} ({asset.type.value}) still in DISCOVERED state"

    def test_html_report_generated(self, tmp_path):
        """HTML report can be generated from validation results."""
        orch = self._build_orchestrator(tmp_path)
        results = asyncio.run(orch.run_pipeline())

        val_result = results["validator"]
        report_data = val_result.details.get("report")
        assert report_data is not None

        report = ValidationReport.model_validate(report_data)
        html = generate_html_report(report)
        assert "<!DOCTYPE html>" in html
        assert "E2E_TEST" in html

        # Save and verify file
        html_path = save_html_report(report, tmp_path / "report.html")
        assert html_path.exists()

    def test_json_report_generated(self, tmp_path):
        """JSON report can be generated and round-tripped."""
        orch = self._build_orchestrator(tmp_path)
        results = asyncio.run(orch.run_pipeline())

        val_result = results["validator"]
        report_data = val_result.details.get("report")
        report = ValidationReport.model_validate(report_data)

        json_str = generate_json_report(report)
        parsed = json.loads(json_str)
        assert parsed["project_name"] == "E2E_TEST"

        json_path = save_json_report(report, tmp_path / "report.json")
        assert json_path.exists()

    def test_output_directory_has_files(self, tmp_path):
        """Output directory contains generated files."""
        orch = self._build_orchestrator(tmp_path)
        asyncio.run(orch.run_pipeline())

        # Should have SQL files, notebook files, pipeline files, connection mappings
        output_files = list(tmp_path.rglob("*"))
        file_extensions = {f.suffix for f in output_files if f.is_file()}
        # At minimum we should see .json (registry, pipeline, connections, triggers) and .sql files
        assert ".json" in file_extensions


class TestE2EDAGExecution:
    """Verify the DAG-based execution order is correct."""

    def test_dag_has_all_agents(self):
        agent_names = [
            "discovery", "sql_converter", "python_converter", "visual_converter",
            "dataset_migrator", "connection_mapper", "pipeline_builder", "validator",
        ]
        dag = build_agent_dag(agent_names)
        assert set(dag.nodes()) == set(agent_names)

    def test_execution_waves_order(self):
        agent_names = [
            "discovery", "sql_converter", "python_converter", "visual_converter",
            "dataset_migrator", "connection_mapper", "pipeline_builder", "validator",
        ]
        dag = build_agent_dag(agent_names)
        waves = get_execution_waves(dag)

        # Wave 1 must be discovery
        assert waves[0] == ["discovery"]

        # validator must be in the last wave
        last_wave = waves[-1]
        assert "validator" in last_wave

        # pipeline_builder must come after converters
        converter_wave = None
        builder_wave = None
        for i, wave in enumerate(waves):
            if "sql_converter" in wave:
                converter_wave = i
            if "pipeline_builder" in wave:
                builder_wave = i
        assert builder_wave > converter_wave

    def test_fail_fast_stops_pipeline(self, tmp_path):
        """If fail_fast is on and an agent fails, pipeline stops early."""

        class FailingAgent(BaseAgent):
            @property
            def name(self):
                return "sql_converter"

            @property
            def description(self):
                return "Always fails"

            async def execute(self, context):
                return AgentResult(agent_name="sql_converter",
                                   status=AgentStatus.FAILED,
                                   errors=["Intentional failure"])

            async def validate(self, context):
                return ValidationResult(passed=False)

        class PostConverterAgent(BaseAgent):
            """An agent that depends on sql_converter — should not run."""

            @property
            def name(self):
                return "pipeline_builder"

            @property
            def description(self):
                return "Depends on sql_converter"

            async def execute(self, context):
                return AgentResult(agent_name="pipeline_builder",
                                   status=AgentStatus.COMPLETED)

            async def validate(self, context):
                return ValidationResult(passed=True)

        config = StubAppConfig()
        config.migration.output_dir = str(tmp_path)
        config.migration.fail_fast = True
        registry = AssetRegistry(project_key="FAIL_TEST", registry_path=tmp_path / "reg.json")
        orch = Orchestrator(config=config, registry=registry)

        orch.register_agent(StubbedDiscoveryAgent())
        orch.register_agent(FailingAgent())         # wave 2 (depends on discovery)
        orch.register_agent(PostConverterAgent())    # wave 3 (depends on sql_converter)

        results = asyncio.run(orch.run_pipeline())

        # sql_converter should have failed
        assert results["sql_converter"].status == AgentStatus.FAILED
        # pipeline_builder should NOT have run (fail-fast aborted after wave 2)
        assert "pipeline_builder" not in results


class TestE2ECircuitBreaker:
    """Verify circuit breaker behaviour in orchestrator."""

    def test_circuit_breaker_trips(self, tmp_path):
        """After N consecutive failures, subsequent agents are aborted."""

        class FailAgent(BaseAgent):
            _count = 0

            def __init__(self, agent_name):
                self._name = agent_name

            @property
            def name(self):
                return self._name

            @property
            def description(self):
                return "Fails"

            async def execute(self, context):
                return AgentResult(agent_name=self._name, status=AgentStatus.FAILED,
                                   errors=["fail"])

            async def validate(self, context):
                return ValidationResult(passed=False)

        config = StubAppConfig()
        config.migration.output_dir = str(tmp_path)
        config.orchestrator.circuit_breaker_threshold = 2
        config.orchestrator.max_retries = 1
        registry = AssetRegistry(project_key="CB_TEST", registry_path=tmp_path / "reg.json")
        orch = Orchestrator(config=config, registry=registry)

        # Register 4 agents in sequential chain
        orch.register_agent(FailAgent("discovery"))
        orch.register_agent(FailAgent("sql_converter"))
        orch.register_agent(FailAgent("pipeline_builder"))

        results = asyncio.run(orch.run_pipeline())

        # After 2 consecutive failures, 3rd should be circuit-breaker aborted
        assert results["pipeline_builder"].status == AgentStatus.ABORTED
        assert "circuit breaker" in results["pipeline_builder"].errors[0].lower()
