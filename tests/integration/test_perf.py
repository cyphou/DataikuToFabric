"""Performance test — verify pipeline runs within acceptable time for large projects.

Generates a synthetic project with 200+ recipes and 50+ datasets,
runs the full pipeline, and asserts it completes within time limits.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

import pytest

from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent, ValidationResult
from src.agents.connection_agent import ConnectionMapperAgent
from src.agents.dataset_agent import DatasetMigrationAgent
from src.agents.flow_pipeline_agent import FlowPipelineAgent
from src.agents.python_migration_agent import PythonMigrationAgent
from src.agents.sql_migration_agent import SQLMigrationAgent
from src.agents.validation_agent import ValidationAgent
from src.agents.visual_recipe_agent import VisualRecipeAgent
from src.core.orchestrator import Orchestrator
from src.core.registry import AssetRegistry
from src.models.asset import Asset, AssetType, MigrationState


# ── Stub config ──────────────────────────────────────────────


@dataclass
class StubFabricConfig:
    workspace_id: str = "perf-test"
    lakehouse_name: str = "lh_perf"
    warehouse_name: str = "wh_perf"
    auth_method: str = "azure_cli"
    tenant_id_env: str = "X"
    client_id_env: str = "X"


@dataclass
class StubOrchestratorConfig:
    max_retries: int = 1
    retry_delay_seconds: int = 0
    agent_timeout_seconds: int = 120
    circuit_breaker_threshold: int = 500
    report_format: list[str] = field(default_factory=lambda: ["json"])


@dataclass
class StubMigrationConfig:
    target_sql_dialect: str = "tsql"
    default_storage: str = "lakehouse"
    parallel_agents: bool = False
    max_concurrent_agents: int = 4
    fail_fast: bool = False
    output_dir: str = ""


@dataclass
class StubDataikuConfig:
    url: str = "http://localhost"
    api_key_env: str = "X"
    project_key: str = "PERF_TEST"
    timeout_seconds: int = 10
    max_retries: int = 1


@dataclass
class StubLoggingConfig:
    level: str = "ERROR"
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


# ── Large project generator ──────────────────────────────────

NUM_SQL_RECIPES = 100
NUM_PYTHON_RECIPES = 60
NUM_VISUAL_RECIPES = 50
NUM_DATASETS = 60
NUM_CONNECTIONS = 10


def _generate_large_project():
    """Build synthetic assets for a 220-recipe, 60-dataset project."""
    assets: list[Asset] = []
    project = "PERF_TEST"

    # SQL recipes
    for i in range(NUM_SQL_RECIPES):
        assets.append(Asset(
            id=f"recipe_sql_{i}",
            type=AssetType.RECIPE_SQL,
            name=f"sql_recipe_{i}",
            source_project=project,
            state=MigrationState.DISCOVERED,
            metadata={
                "recipe_type": "sql",
                "payload": f"SELECT col_{i}, col_{i+1} FROM table_{i} WHERE active = 1",
                "connection": f"conn_{i % NUM_CONNECTIONS}",
            },
        ))

    # Python recipes
    for i in range(NUM_PYTHON_RECIPES):
        assets.append(Asset(
            id=f"recipe_py_{i}",
            type=AssetType.RECIPE_PYTHON,
            name=f"python_recipe_{i}",
            source_project=project,
            state=MigrationState.DISCOVERED,
            metadata={
                "recipe_type": "python",
                "payload": f"import dataiku\nds = dataiku.Dataset('input_{i}')\ndf = ds.get_dataframe()\ndf['new_col'] = df['col_{i}'] * 2\ndataiku.Dataset('output_{i}').write_dataframe(df)",
            },
        ))

    # Visual recipes — mix of joins, groups, filters
    visual_types = ["join", "group", "filter", "sort", "distinct"]
    for i in range(NUM_VISUAL_RECIPES):
        vtype = visual_types[i % len(visual_types)]
        if vtype == "join":
            payload = {"join": {"inputs": [{"dataset": f"ds_{i}", "alias": "a"}, {"dataset": f"ds_{i+1}", "alias": "b"}], "on": [{"left": "a.id", "right": "b.id"}], "type": "INNER"}}
        elif vtype == "group":
            payload = {"group": {"keys": [{"column": "category"}], "aggregations": [{"column": "value", "function": "SUM"}]}}
        elif vtype == "filter":
            payload = {"filter": {"conditions": [{"column": "status", "operator": "==", "value": "active"}]}}
        elif vtype == "sort":
            payload = {"sort": {"columns": [{"column": "created_at", "order": "desc"}]}}
        else:
            payload = {"distinct": {"columns": [{"column": "id"}]}}

        assets.append(Asset(
            id=f"recipe_vis_{i}",
            type=AssetType.RECIPE_VISUAL,
            name=f"visual_recipe_{i}",
            source_project=project,
            state=MigrationState.DISCOVERED,
            metadata={"recipe_type": vtype, "payload": payload},
        ))

    # Datasets
    for i in range(NUM_DATASETS):
        assets.append(Asset(
            id=f"dataset_{i}",
            type=AssetType.DATASET,
            name=f"dataset_{i}",
            source_project=project,
            state=MigrationState.DISCOVERED,
            metadata={
                "schema": {"columns": [
                    {"name": f"col_{j}", "type": "string"} for j in range(10)
                ]},
                "managed": True,
            },
        ))

    # Connections
    for i in range(NUM_CONNECTIONS):
        conn_types = ["Oracle", "PostgreSQL", "S3", "HDFS", "SFTP",
                       "MySQL", "Redshift", "BigQuery", "Snowflake", "Azure_SQL"]
        assets.append(Asset(
            id=f"connection_{i}",
            type=AssetType.CONNECTION,
            name=f"connection_{i}",
            source_project=project,
            state=MigrationState.DISCOVERED,
            metadata={"type": conn_types[i % len(conn_types)], "params": {"host": f"host_{i}"}},
        ))

    # One flow with a 4-node graph
    assets.append(Asset(
        id="flow_main",
        type=AssetType.FLOW,
        name="main_flow",
        source_project=project,
        state=MigrationState.DISCOVERED,
        metadata={
            "graph": {
                "items": {
                    "step_a": {"type": "recipe.sql"},
                    "step_b": {"type": "recipe.python"},
                    "step_c": {"type": "recipe.visual"},
                    "step_d": {"type": "recipe.sql"},
                },
                "edges": [
                    {"from": "step_a", "to": "step_b"},
                    {"from": "step_b", "to": "step_c"},
                    {"from": "step_c", "to": "step_d"},
                ],
            },
        },
    ))

    # One scenario
    assets.append(Asset(
        id="scenario_main",
        type=AssetType.SCENARIO,
        name="main_scenario",
        source_project=project,
        state=MigrationState.DISCOVERED,
        metadata={"triggers": [{"type": "temporal", "params": {"frequency": "hourly"}}], "steps": []},
    ))

    return assets


# ── Stubbed discovery agent that loads pre-built assets ──────


class PerfDiscoveryAgent(BaseAgent):
    """Loads pre-generated assets into registry."""

    def __init__(self, assets: list[Asset]):
        self._assets = assets

    @property
    def name(self) -> str:
        return "discovery"

    @property
    def description(self) -> str:
        return "Perf test discovery"

    async def execute(self, context: Any) -> AgentResult:
        for asset in self._assets:
            context.registry.add_asset(asset)
        context.registry.save()
        return AgentResult(
            agent_name=self.name,
            status=AgentStatus.COMPLETED,
            assets_processed=len(self._assets),
        )

    async def validate(self, context: Any) -> ValidationResult:
        return ValidationResult(passed=True, checks_run=1, checks_passed=1)


# ── Tests ─────────────────────────────────────────────────────


class TestPerformance:
    def _build_orchestrator(self, tmp_path, assets):
        config = StubAppConfig()
        config.migration.output_dir = str(tmp_path)
        registry = AssetRegistry(project_key="PERF_TEST", registry_path=tmp_path / "registry.json")
        orch = Orchestrator(config=config, registry=registry)

        orch.register_agent(PerfDiscoveryAgent(assets))
        orch.register_agent(SQLMigrationAgent())
        orch.register_agent(PythonMigrationAgent())
        orch.register_agent(VisualRecipeAgent())
        orch.register_agent(DatasetMigrationAgent())
        orch.register_agent(ConnectionMapperAgent())
        orch.register_agent(FlowPipelineAgent())
        orch.register_agent(ValidationAgent())

        return orch

    def test_large_project_completes(self, tmp_path):
        """220+ recipe, 60 dataset project completes without errors."""
        assets = _generate_large_project()
        total_assets = len(assets)
        assert total_assets > 280  # sanity check

        orch = self._build_orchestrator(tmp_path, assets)
        results = asyncio.run(orch.run_pipeline())

        # All agents should complete
        for name, result in results.items():
            assert result.status in (AgentStatus.COMPLETED,), \
                f"Agent {name} status={result.status.value}: {result.errors}"

    def test_large_project_under_time_limit(self, tmp_path):
        """Full pipeline on 280+ assets finishes in < 30s."""
        assets = _generate_large_project()
        orch = self._build_orchestrator(tmp_path, assets)

        start = time.perf_counter()
        results = asyncio.run(orch.run_pipeline())
        elapsed = time.perf_counter() - start

        assert elapsed < 30.0, f"Pipeline took {elapsed:.1f}s (limit 30s)"

    def test_large_project_asset_counts(self, tmp_path):
        """Check correct asset processing counts after large-project run."""
        assets = _generate_large_project()
        orch = self._build_orchestrator(tmp_path, assets)
        results = asyncio.run(orch.run_pipeline())

        # Discovery should have loaded all assets
        assert results["discovery"].assets_processed == len(assets)

        # SQL converter should have processed exactly NUM_SQL_RECIPES
        assert results["sql_converter"].assets_processed == NUM_SQL_RECIPES

        # Python converter should have processed exactly NUM_PYTHON_RECIPES
        assert results["python_converter"].assets_processed == NUM_PYTHON_RECIPES

    def test_memory_stays_reasonable(self, tmp_path):
        """Peak memory stays under 200MB for a large project."""
        import tracemalloc

        tracemalloc.start()
        assets = _generate_large_project()
        orch = self._build_orchestrator(tmp_path, assets)
        asyncio.run(orch.run_pipeline())

        _, peak_mb = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_mb_val = peak_mb / (1024 * 1024)
        assert peak_mb_val < 200, f"Peak memory {peak_mb_val:.1f}MB exceeds 200MB limit"
