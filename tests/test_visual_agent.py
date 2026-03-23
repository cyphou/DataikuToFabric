"""Visual recipe agent integration tests — Prepare, Pivot, and agent flow."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from src.agents.base_agent import AgentStatus
from src.agents.visual_recipe_agent import (
    RECIPE_GENERATORS,
    VisualRecipeAgent,
    _generate_pivot_sql,
    _generate_prepare_sql,
)
from src.core.registry import AssetRegistry
from src.models.asset import Asset, AssetType, MigrationState


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


def _make_visual_asset(name: str, recipe_type: str, params: dict) -> Asset:
    return Asset(
        id=f"recipe_visual_{name}",
        name=name,
        type=AssetType.RECIPE_VISUAL,
        source_project=PROJECT,
        state=MigrationState.DISCOVERED,
        metadata={"recipe_type": recipe_type, "params": params},
    )


# ═══════════════════════════════════════════════════════════════════════════
# A) Pivot generator  (5 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestPivotGenerator:
    def test_basic_pivot(self):
        params = {
            "input": "sales",
            "pivotColumn": "region",
            "valueColumn": "amount",
            "aggregation": "SUM",
            "pivotValues": ["East", "West", "North"],
            "groupColumns": ["product"],
        }
        sql = _generate_pivot_sql(params)
        assert "PIVOT" in sql.upper()
        assert "SUM" in sql.upper()
        assert "[East]" in sql
        assert "[West]" in sql
        assert "[product]" in sql

    def test_pivot_no_group_columns(self):
        params = {
            "input": "data",
            "pivotColumn": "cat",
            "valueColumn": "val",
            "aggregation": "COUNT",
            "pivotValues": ["A", "B"],
        }
        sql = _generate_pivot_sql(params)
        assert "PIVOT" in sql.upper()
        assert "COUNT" in sql.upper()
        assert "[A]" in sql

    def test_pivot_no_values_fallback(self):
        params = {
            "input": "data",
            "pivotColumn": "cat",
            "valueColumn": "val",
            "pivotValues": [],
        }
        sql = _generate_pivot_sql(params)
        assert "add pivot values" in sql.lower() or "add values" in sql.lower()

    def test_pivot_avg_aggregation(self):
        params = {
            "input": "scores",
            "pivotColumn": "subject",
            "valueColumn": "score",
            "aggregation": "AVG",
            "pivotValues": ["Math", "Science"],
            "groupColumns": ["student_id"],
        }
        sql = _generate_pivot_sql(params)
        assert "AVG" in sql.upper()

    def test_pivot_in_generators(self):
        assert "pivot" in RECIPE_GENERATORS
        assert callable(RECIPE_GENERATORS["pivot"])


# ═══════════════════════════════════════════════════════════════════════════
# B) Prepare generator  (10 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestPrepareGenerator:
    def test_empty_steps(self):
        params = {"input": "raw_data", "steps": []}
        sql = _generate_prepare_sql(params)
        assert "SELECT *" in sql.upper()
        assert "raw_data" in sql

    def test_filter_step(self):
        params = {
            "input": "orders",
            "steps": [{"type": "filter", "column": "amount", "operator": ">", "value": "100"}],
        }
        sql = _generate_prepare_sql(params)
        assert "WHERE" in sql.upper()
        assert "amount" in sql

    def test_rename_step(self):
        params = {
            "input": "users",
            "steps": [{"type": "rename", "column": "name", "newName": "full_name"}],
        }
        sql = _generate_prepare_sql(params)
        assert "Rename" in sql

    def test_delete_column_step(self):
        params = {
            "input": "data",
            "steps": [{"type": "delete_column", "column": "temp_col"}],
        }
        sql = _generate_prepare_sql(params)
        assert "Drop" in sql or "drop" in sql.lower()
        assert "temp_col" in sql

    def test_fill_empty_step(self):
        params = {
            "input": "data",
            "steps": [{"type": "fill_empty", "column": "name", "value": "Unknown"}],
        }
        sql = _generate_prepare_sql(params)
        assert "ISNULL" in sql.upper()
        assert "Unknown" in sql

    def test_uppercase_step(self):
        params = {
            "input": "data",
            "steps": [{"type": "uppercase", "column": "city"}],
        }
        sql = _generate_prepare_sql(params)
        assert "UPPER" in sql.upper()

    def test_lowercase_step(self):
        params = {
            "input": "data",
            "steps": [{"type": "lowercase", "column": "email"}],
        }
        sql = _generate_prepare_sql(params)
        assert "LOWER" in sql.upper()

    def test_formula_step(self):
        params = {
            "input": "data",
            "steps": [{"type": "formula", "expression": "price * qty", "outputColumn": "total"}],
        }
        sql = _generate_prepare_sql(params)
        assert "price * qty" in sql
        assert "total" in sql

    def test_find_replace_step(self):
        params = {
            "input": "data",
            "steps": [{"type": "find_replace", "column": "status", "find": "Y", "replace": "Yes"}],
        }
        sql = _generate_prepare_sql(params)
        assert "REPLACE" in sql.upper()

    def test_type_change_step(self):
        params = {
            "input": "data",
            "steps": [{"type": "type_change", "column": "amount", "newType": "DECIMAL(10,2)"}],
        }
        sql = _generate_prepare_sql(params)
        assert "CAST" in sql.upper()
        assert "DECIMAL" in sql.upper()

    def test_split_column_unsupported_comment(self):
        params = {
            "input": "data",
            "steps": [{"type": "split_column", "column": "full_name", "delimiter": " "}],
        }
        sql = _generate_prepare_sql(params)
        assert "SPLIT" in sql.upper() or "split" in sql.lower()

    def test_unknown_step_commented(self):
        params = {
            "input": "data",
            "steps": [{"type": "custom_magic"}],
        }
        sql = _generate_prepare_sql(params)
        assert "Unsupported" in sql or "unsupported" in sql.lower()

    def test_multiple_steps(self):
        params = {
            "input": "orders",
            "steps": [
                {"type": "filter", "column": "amount", "operator": ">", "value": "0"},
                {"type": "uppercase", "column": "status"},
                {"type": "fill_empty", "column": "note", "value": "N/A"},
            ],
        }
        sql = _generate_prepare_sql(params)
        assert "WHERE" in sql.upper()
        assert "UPPER" in sql.upper()
        assert "ISNULL" in sql.upper()

    def test_prepare_in_generators(self):
        assert "prepare" in RECIPE_GENERATORS
        assert callable(RECIPE_GENERATORS["prepare"])


# ═══════════════════════════════════════════════════════════════════════════
# C) Agent execute flow  (5 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestVisualAgentExecute:
    @pytest.fixture
    def setup(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        reg.add_asset(_make_visual_asset("join1", "join", {
            "joinType": "INNER",
            "leftInput": "orders",
            "rightInput": "customers",
            "conditions": [{"leftColumn": "cust_id", "rightColumn": "id"}],
        }))
        reg.add_asset(_make_visual_asset("prep1", "prepare", {
            "input": "raw",
            "steps": [{"type": "uppercase", "column": "name"}],
        }))
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        return ctx, reg

    @pytest.mark.asyncio
    async def test_converts_visual_recipes(self, setup):
        ctx, _ = setup
        agent = VisualRecipeAgent()
        result = await agent.execute(ctx)
        assert result.status == AgentStatus.COMPLETED
        assert result.assets_converted == 2
        assert result.assets_failed == 0

    @pytest.mark.asyncio
    async def test_creates_sql_files(self, setup):
        ctx, reg = setup
        agent = VisualRecipeAgent()
        await agent.execute(ctx)
        for aid in ["recipe_visual_join1", "recipe_visual_prep1"]:
            asset = reg.get_asset(aid)
            path = Path(asset.target_fabric_asset["path"])
            assert path.exists()

    @pytest.mark.asyncio
    async def test_state_updated(self, setup):
        ctx, reg = setup
        agent = VisualRecipeAgent()
        await agent.execute(ctx)
        for aid in ["recipe_visual_join1", "recipe_visual_prep1"]:
            asset = reg.get_asset(aid)
            assert asset.state == MigrationState.CONVERTED

    @pytest.mark.asyncio
    async def test_unsupported_type_fails(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        reg.add_asset(_make_visual_asset("unknown", "custom_magic", {}))
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        agent = VisualRecipeAgent()
        result = await agent.execute(ctx)
        assert result.assets_failed == 1

    @pytest.mark.asyncio
    async def test_pivot_via_agent(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        reg.add_asset(_make_visual_asset("piv1", "pivot", {
            "input": "sales",
            "pivotColumn": "region",
            "valueColumn": "amount",
            "aggregation": "SUM",
            "pivotValues": ["East", "West"],
            "groupColumns": ["product"],
        }))
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        agent = VisualRecipeAgent()
        result = await agent.execute(ctx)
        assert result.assets_converted == 1
        asset = reg.get_asset("recipe_visual_piv1")
        content = Path(asset.target_fabric_asset["path"]).read_text()
        assert "PIVOT" in content.upper()


# ═══════════════════════════════════════════════════════════════════════════
# D) Agent validate  (2 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestVisualAgentValidate:
    @pytest.mark.asyncio
    async def test_validate_passes(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        reg.add_asset(_make_visual_asset("f1", "filter", {
            "input": "t",
            "conditions": [{"column": "x", "operator": "=", "value": "1"}],
        }))
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        agent = VisualRecipeAgent()
        await agent.execute(ctx)
        vr = await agent.validate(ctx)
        assert vr.passed is True

    @pytest.mark.asyncio
    async def test_validate_missing_file(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        asset = _make_visual_asset("miss", "join", {})
        reg.add_asset(asset)
        reg.update_state(asset.id, MigrationState.CONVERTED)
        reg.set_target(asset.id, {"type": "sql_script", "path": "/nonexistent.sql"})
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        agent = VisualRecipeAgent()
        vr = await agent.validate(ctx)
        assert vr.passed is False


# ═══════════════════════════════════════════════════════════════════════════
# E) Generator registry completeness  (2 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestGeneratorRegistry:
    def test_all_10_generators(self):
        expected = {"join", "vstack", "group", "filter", "window", "sort", "distinct", "topn", "pivot", "prepare"}
        assert expected == set(RECIPE_GENERATORS.keys())

    def test_all_generators_callable(self):
        for name, gen in RECIPE_GENERATORS.items():
            assert callable(gen), f"Generator '{name}' is not callable"
