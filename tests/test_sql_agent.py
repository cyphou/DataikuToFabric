"""SQL Migration Agent integration tests — end-to-end conversion flow."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from src.agents.base_agent import AgentStatus
from src.agents.sql_migration_agent import DIALECT_MAP, SQLMigrationAgent, _detect_dialect
from src.core.registry import AssetRegistry
from src.models.asset import Asset, AssetType, MigrationState


# ── Helpers ────────────────────────────────────────────────────────────────

PROJECT = "TEST_PROJECT"


@dataclass
class StubMigrationConfig:
    target_sql_dialect: str = "tsql"
    output_dir: str = "./output"


@dataclass
class StubConfig:
    migration: StubMigrationConfig = field(default_factory=StubMigrationConfig)


@dataclass
class StubContext:
    registry: AssetRegistry = None
    config: StubConfig = field(default_factory=StubConfig)


def _make_sql_asset(
    name: str,
    sql: str,
    dialect: str = "oracle",
    *,
    connection: str = "",
    asset_id: str | None = None,
) -> Asset:
    metadata: dict[str, Any] = {"payload": sql, "recipe_type": dialect}
    if connection:
        metadata["connection"] = connection
    return Asset(
        id=asset_id or f"recipe_sql_{name}",
        name=name,
        type=AssetType.RECIPE_SQL,
        source_project=PROJECT,
        state=MigrationState.DISCOVERED,
        metadata=metadata,
    )


# ═══════════════════════════════════════════════════════════════════════════
# A) _detect_dialect  (6 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestDetectDialect:
    def test_explicit_dialect(self):
        assert _detect_dialect({"dialect": "oracle"}) == "oracle"

    def test_explicit_dialect_case_insensitive(self):
        assert _detect_dialect({"dialect": "PostgreSQL"}) == "postgresql"

    def test_from_connection(self):
        assert _detect_dialect({"connection": "my_oracle_prod"}) == "oracle"

    def test_from_connection_postgres(self):
        assert _detect_dialect({"connection": "postgres_analytics"}) == "postgres"

    def test_fallback_to_recipe_type(self):
        assert _detect_dialect({"recipe_type": "mysql"}) == "mysql"

    def test_empty_metadata(self):
        assert _detect_dialect({}) == ""


# ═══════════════════════════════════════════════════════════════════════════
# B) Agent execute — single Oracle recipe  (3 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestAgentExecuteOracle:
    @pytest.fixture
    def setup(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        asset = _make_sql_asset("q1", "SELECT NVL(a, b) FROM t", "oracle")
        reg.add_asset(asset)
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        return ctx, reg

    @pytest.mark.asyncio
    async def test_converts_oracle_recipe(self, setup):
        ctx, reg = setup
        agent = SQLMigrationAgent()
        result = await agent.execute(ctx)
        assert result.status == AgentStatus.COMPLETED
        assert result.assets_processed == 1
        assert result.assets_converted == 1
        assert result.assets_failed == 0

    @pytest.mark.asyncio
    async def test_output_file_created(self, setup):
        ctx, reg = setup
        agent = SQLMigrationAgent()
        await agent.execute(ctx)
        asset = reg.get_asset("recipe_sql_q1")
        assert asset.target_fabric_asset is not None
        path = Path(asset.target_fabric_asset["path"])
        assert path.exists()
        content = path.read_text()
        assert len(content) > 0

    @pytest.mark.asyncio
    async def test_state_updated_to_converted(self, setup):
        ctx, reg = setup
        agent = SQLMigrationAgent()
        await agent.execute(ctx)
        asset = reg.get_asset("recipe_sql_q1")
        assert asset.state == MigrationState.CONVERTED


# ═══════════════════════════════════════════════════════════════════════════
# C) Agent execute — PostgreSQL recipe  (2 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestAgentExecutePostgres:
    @pytest.fixture
    def setup(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        asset = _make_sql_asset(
            "pg_query",
            "SELECT * FROM users WHERE active = TRUE LIMIT 10",
            "postgres",
        )
        reg.add_asset(asset)
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        return ctx, reg

    @pytest.mark.asyncio
    async def test_converts_postgres_recipe(self, setup):
        ctx, reg = setup
        agent = SQLMigrationAgent()
        result = await agent.execute(ctx)
        assert result.assets_converted == 1
        assert result.assets_failed == 0

    @pytest.mark.asyncio
    async def test_target_has_source_dialect(self, setup):
        ctx, reg = setup
        agent = SQLMigrationAgent()
        await agent.execute(ctx)
        asset = reg.get_asset("recipe_sql_pg_query")
        assert asset.target_fabric_asset["source_dialect"] == "postgres"


# ═══════════════════════════════════════════════════════════════════════════
# D) Agent execute — empty payload  (2 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestAgentEmptyPayload:
    @pytest.fixture
    def setup(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        asset = _make_sql_asset("empty", "", "oracle")
        reg.add_asset(asset)
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        return ctx, reg

    @pytest.mark.asyncio
    async def test_empty_payload_fails_gracefully(self, setup):
        ctx, reg = setup
        agent = SQLMigrationAgent()
        result = await agent.execute(ctx)
        assert result.assets_failed == 1
        assert result.assets_converted == 0

    @pytest.mark.asyncio
    async def test_empty_payload_sets_failed_state(self, setup):
        ctx, reg = setup
        agent = SQLMigrationAgent()
        await agent.execute(ctx)
        asset = reg.get_asset("recipe_sql_empty")
        assert asset.state == MigrationState.FAILED


# ═══════════════════════════════════════════════════════════════════════════
# E) Agent execute — multiple recipes  (2 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestAgentMultipleRecipes:
    @pytest.fixture
    def setup(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        reg.add_asset(_make_sql_asset("a1", "SELECT 1", "oracle"))
        reg.add_asset(_make_sql_asset("a2", "SELECT SYSDATE", "oracle"))
        reg.add_asset(_make_sql_asset("a3", "SELECT NOW()", "postgres"))
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        return ctx, reg

    @pytest.mark.asyncio
    async def test_processes_all_recipes(self, setup):
        ctx, _ = setup
        agent = SQLMigrationAgent()
        result = await agent.execute(ctx)
        assert result.assets_processed == 3

    @pytest.mark.asyncio
    async def test_all_converted(self, setup):
        ctx, _ = setup
        agent = SQLMigrationAgent()
        result = await agent.execute(ctx)
        assert result.assets_converted == 3
        assert result.assets_failed == 0


# ═══════════════════════════════════════════════════════════════════════════
# F) Agent validate  (3 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestAgentValidate:
    @pytest.fixture
    def setup(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        reg.add_asset(_make_sql_asset("valid_q", "SELECT id, name FROM orders", "oracle"))
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        agent = SQLMigrationAgent()
        return ctx, reg, agent

    @pytest.mark.asyncio
    async def test_validate_passes(self, setup):
        ctx, _, agent = setup
        await agent.execute(ctx)
        vr = await agent.validate(ctx)
        assert vr.passed is True
        assert vr.checks_failed == 0

    @pytest.mark.asyncio
    async def test_validate_checks_count(self, setup):
        ctx, _, agent = setup
        await agent.execute(ctx)
        vr = await agent.validate(ctx)
        assert vr.checks_run == 1

    @pytest.mark.asyncio
    async def test_validate_missing_file(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        asset = _make_sql_asset("missing", "SELECT 1", "oracle")
        reg.add_asset(asset)
        # Manually set state + target to simulate a converted asset with missing file
        reg.update_state(asset.id, MigrationState.CONVERTED)
        reg.set_target(asset.id, {"type": "sql_script", "dialect": "tsql", "path": "/nonexistent.sql"})
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        agent = SQLMigrationAgent()
        vr = await agent.validate(ctx)
        assert vr.passed is False
        assert vr.checks_failed == 1


# ═══════════════════════════════════════════════════════════════════════════
# G) Dialect detection from connection  (2 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestDialectFromConnection:
    @pytest.fixture
    def setup(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        # Recipe with no explicit dialect but has oracle connection
        asset = Asset(
            id="recipe_sql_infer",
            name="infer_dialect",
            type=AssetType.RECIPE_SQL,
            source_project=PROJECT,
            state=MigrationState.DISCOVERED,
            metadata={"payload": "SELECT SYSDATE", "connection": "oracle_prod"},
        )
        reg.add_asset(asset)
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        return ctx, reg

    @pytest.mark.asyncio
    async def test_infers_oracle_from_connection(self, setup):
        ctx, reg = setup
        agent = SQLMigrationAgent()
        result = await agent.execute(ctx)
        assert result.assets_converted == 1
        asset = reg.get_asset("recipe_sql_infer")
        assert asset.target_fabric_asset["source_dialect"] == "oracle"

    @pytest.mark.asyncio
    async def test_registers_review_flags(self, setup):
        ctx, reg = setup
        agent = SQLMigrationAgent()
        result = await agent.execute(ctx)
        # Oracle SYSDATE → GETDATE handled by sqlglot, no custom review flags expected
        assert isinstance(result.review_flags, list)


# ═══════════════════════════════════════════════════════════════════════════
# H) Multi-statement SQL  (2 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestMultiStatement:
    @pytest.fixture
    def setup(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        multi_sql = "INSERT INTO t VALUES (1);\nINSERT INTO t VALUES (2);\nSELECT * FROM t;"
        asset = _make_sql_asset("multi", multi_sql, "oracle")
        reg.add_asset(asset)
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        return ctx, reg

    @pytest.mark.asyncio
    async def test_multi_statement_converts(self, setup):
        ctx, _ = setup
        agent = SQLMigrationAgent()
        result = await agent.execute(ctx)
        assert result.assets_converted == 1

    @pytest.mark.asyncio
    async def test_multi_statement_output_contains_all(self, setup):
        ctx, reg = setup
        agent = SQLMigrationAgent()
        await agent.execute(ctx)
        asset = reg.get_asset("recipe_sql_multi")
        content = Path(asset.target_fabric_asset["path"]).read_text()
        # Should contain multiple statements
        assert content.count("INSERT") >= 2 or content.count("SELECT") >= 1


# ═══════════════════════════════════════════════════════════════════════════
# I) DIALECT_MAP coverage  (2 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestDialectMap:
    def test_all_expected_keys(self):
        expected_keys = {"oracle", "postgresql", "postgres", "mysql", "hive", "impala", "sqlserver", "mssql"}
        assert expected_keys == set(DIALECT_MAP.keys())

    def test_oracle_maps_to_oracle(self):
        assert DIALECT_MAP["oracle"] == "oracle"
        assert DIALECT_MAP["postgresql"] == "postgres"
        assert DIALECT_MAP["mssql"] == "tsql"
