"""Python migration agent + translator integration tests."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from src.agents.base_agent import AgentStatus
from src.agents.python_migration_agent import PythonMigrationAgent
from src.core.registry import AssetRegistry
from src.models.asset import Asset, AssetType, MigrationState
from src.translators.python_to_notebook import (
    convert_python_to_pyspark,
    detect_dataiku_sdk_usage,
    detect_pandas_usage,
    generate_notebook,
)


PROJECT = "TEST_PROJECT"


@dataclass
class StubDataikuConfig:
    project_key: str = "TEST_PROJECT"


@dataclass
class StubMigrationConfig:
    target_sql_dialect: str = "tsql"
    output_dir: str = "./output"


@dataclass
class StubConfig:
    dataiku: StubDataikuConfig = field(default_factory=StubDataikuConfig)
    migration: StubMigrationConfig = field(default_factory=StubMigrationConfig)


@dataclass
class StubContext:
    registry: AssetRegistry = None
    config: StubConfig = field(default_factory=StubConfig)


def _make_python_asset(name: str, code: str, **extra_meta) -> Asset:
    metadata: dict[str, Any] = {"payload": code, **extra_meta}
    return Asset(
        id=f"recipe_python_{name}",
        name=name,
        type=AssetType.RECIPE_PYTHON,
        source_project=PROJECT,
        state=MigrationState.DISCOVERED,
        metadata=metadata,
    )


# ═══════════════════════════════════════════════════════════════════════════
# A) Expanded SDK detection (AST + regex)  (6 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestExpandedDetection:
    def test_ast_detects_import(self):
        code = "import dataiku\nprint(1)"
        found = detect_dataiku_sdk_usage(code)
        assert any("dataiku" in f for f in found)

    def test_ast_detects_from_import(self):
        code = "from dataiku.core import Dataset"
        found = detect_dataiku_sdk_usage(code)
        assert any("dataiku" in f for f in found)

    def test_ast_detects_attribute(self):
        code = 'ds = dataiku.Dataset("x")'
        found = detect_dataiku_sdk_usage(code)
        assert any("Dataset" in f for f in found)

    def test_detects_sql_executor(self):
        code = 'executor = dataiku.SQLExecutor2("conn1")'
        found = detect_dataiku_sdk_usage(code)
        assert any("SQLExecutor2" in f for f in found)

    def test_detects_default_project_key(self):
        code = "key = dataiku.default_project_key()"
        found = detect_dataiku_sdk_usage(code)
        assert any("default_project_key" in f for f in found)

    def test_syntax_error_falls_back_to_regex(self):
        code = 'dataiku.Dataset("x" +'  # invalid syntax
        found = detect_dataiku_sdk_usage(code)
        assert any("Dataset" in f for f in found)


# ═══════════════════════════════════════════════════════════════════════════
# B) Pandas detection  (4 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestPandasDetection:
    def test_detects_read_csv(self):
        code = "import pandas as pd\ndf = pd.read_csv('file.csv')"
        found = detect_pandas_usage(code)
        assert "pd.read_csv" in found

    def test_detects_to_parquet(self):
        code = "df.to_parquet('out.parquet')"
        found = detect_pandas_usage(code)
        assert "df.to_parquet" in found

    def test_detects_groupby(self):
        code = "result = df.groupby('col').sum()"
        found = detect_pandas_usage(code)
        assert "df.groupby" in found

    def test_nothing_for_pyspark(self):
        code = "df = spark.read.format('delta').load('Tables/x')"
        found = detect_pandas_usage(code)
        assert found == []


# ═══════════════════════════════════════════════════════════════════════════
# C) Expanded SDK replacements  (8 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestExpandedReplacements:
    def test_dataset_get_metadata(self):
        code = 'meta = dataiku.Dataset("orders").get_metadata()'
        result, _, count = convert_python_to_pyspark(code)
        assert "schema" in result
        assert count >= 1

    def test_dataset_read_schema(self):
        code = 'schema = dataiku.Dataset("orders").read_schema()'
        result, _, count = convert_python_to_pyspark(code)
        assert "schema" in result
        assert count >= 1

    def test_folder_list_paths(self):
        code = 'paths = dataiku.Folder("uploads").list_paths_in_partition()'
        result, _, count = convert_python_to_pyspark(code)
        assert "mssparkutils" in result
        assert count >= 1

    def test_get_custom_variables(self):
        code = "vars = dataiku.get_custom_variables()"
        result, _, count = convert_python_to_pyspark(code)
        assert "spark.conf" in result
        assert count >= 1

    def test_sql_executor(self):
        code = 'ex = dataiku.SQLExecutor2("conn")'
        result, _, count = convert_python_to_pyspark(code)
        assert "spark" in result
        assert count >= 1

    def test_from_dataiku_core_import(self):
        code = "from dataiku.core import default_project_key"
        result, _, count = convert_python_to_pyspark(code)
        assert "Dataiku" in result or "# " in result
        assert count >= 1

    def test_default_project_key(self):
        code = "key = dataiku.default_project_key()"
        result, _, count = convert_python_to_pyspark(code)
        assert "PROJECT_KEY" in result
        assert count >= 1

    def test_flags_sklearn(self):
        code = "from sklearn.ensemble import RandomForestClassifier"
        _, flags, _ = convert_python_to_pyspark(code)
        assert any("scikit-learn" in f for f in flags)


# ═══════════════════════════════════════════════════════════════════════════
# D) Pandas → PySpark replacements  (5 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestPandasReplacements:
    def test_read_csv(self):
        code = "df = pd.read_csv('data.csv')"
        result, flags, count = convert_python_to_pyspark(code)
        assert "spark.read.csv" in result
        assert count >= 1
        assert any("Pandas" in f for f in flags)

    def test_read_parquet(self):
        code = "df = pd.read_parquet('data.parquet')"
        result, _, count = convert_python_to_pyspark(code)
        assert "spark.read.parquet" in result
        assert count >= 1

    def test_to_csv(self):
        code = "df.to_csv('out.csv', index=False)"
        result, _, count = convert_python_to_pyspark(code)
        assert "write" in result
        assert "csv" in result
        assert count >= 1

    def test_drop_duplicates(self):
        code = "df.drop_duplicates()"
        result, _, count = convert_python_to_pyspark(code)
        assert "dropDuplicates" in result
        assert count >= 1

    def test_to_parquet(self):
        code = "df.to_parquet('out.parquet')"
        result, _, count = convert_python_to_pyspark(code)
        assert "write" in result
        assert "parquet" in result


# ═══════════════════════════════════════════════════════════════════════════
# E) Agent execute flow  (6 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestPythonAgentExecute:
    @pytest.fixture
    def setup(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        asset = _make_python_asset(
            "transform",
            'import dataiku\ndf = dataiku.Dataset("orders").get_dataframe()\ndf.to_csv("out.csv")',
            inputs=["orders"],
            outputs=["result"],
        )
        reg.add_asset(asset)
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        return ctx, reg

    @pytest.mark.asyncio
    async def test_converts_recipe(self, setup):
        ctx, _ = setup
        agent = PythonMigrationAgent()
        result = await agent.execute(ctx)
        assert result.status == AgentStatus.COMPLETED
        assert result.assets_converted == 1
        assert result.assets_failed == 0

    @pytest.mark.asyncio
    async def test_creates_notebook_file(self, setup):
        ctx, reg = setup
        agent = PythonMigrationAgent()
        await agent.execute(ctx)
        asset = reg.get_asset("recipe_python_transform")
        path = Path(asset.target_fabric_asset["path"])
        assert path.exists()
        nb = json.loads(path.read_text())
        assert nb["nbformat"] == 4

    @pytest.mark.asyncio
    async def test_state_updated(self, setup):
        ctx, reg = setup
        agent = PythonMigrationAgent()
        await agent.execute(ctx)
        asset = reg.get_asset("recipe_python_transform")
        assert asset.state == MigrationState.CONVERTED

    @pytest.mark.asyncio
    async def test_empty_payload_fails(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        reg.add_asset(_make_python_asset("empty", ""))
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        agent = PythonMigrationAgent()
        result = await agent.execute(ctx)
        assert result.assets_failed == 1

    @pytest.mark.asyncio
    async def test_multiple_recipes(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        reg.add_asset(_make_python_asset("a", "print(1)"))
        reg.add_asset(_make_python_asset("b", 'import dataiku\ndf = dataiku.Dataset("x").get_dataframe()'))
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        agent = PythonMigrationAgent()
        result = await agent.execute(ctx)
        assert result.assets_processed == 2
        assert result.assets_converted == 2

    @pytest.mark.asyncio
    async def test_replacements_tracked_in_target(self, setup):
        ctx, reg = setup
        agent = PythonMigrationAgent()
        await agent.execute(ctx)
        asset = reg.get_asset("recipe_python_transform")
        assert asset.target_fabric_asset["replacements_made"] >= 1


# ═══════════════════════════════════════════════════════════════════════════
# F) Agent validate  (3 tests)
# ═══════════════════════════════════════════════════════════════════════════


class TestPythonAgentValidate:
    @pytest.mark.asyncio
    async def test_validate_passes(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        reg.add_asset(_make_python_asset("ok", "print('hello')"))
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        agent = PythonMigrationAgent()
        await agent.execute(ctx)
        vr = await agent.validate(ctx)
        assert vr.passed is True

    @pytest.mark.asyncio
    async def test_validate_missing_file(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        asset = _make_python_asset("missing", "x = 1")
        reg.add_asset(asset)
        reg.update_state(asset.id, MigrationState.CONVERTED)
        reg.set_target(asset.id, {"type": "notebook", "path": "/nonexistent.ipynb"})
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        agent = PythonMigrationAgent()
        vr = await agent.validate(ctx)
        assert vr.passed is False

    @pytest.mark.asyncio
    async def test_validate_checks_count(self, tmp_path):
        reg = AssetRegistry(project_key=PROJECT, registry_path=tmp_path / "reg.json")
        reg.add_asset(_make_python_asset("v1", "print(1)"))
        reg.add_asset(_make_python_asset("v2", "print(2)"))
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        ctx = StubContext(registry=reg, config=cfg)
        agent = PythonMigrationAgent()
        await agent.execute(ctx)
        vr = await agent.validate(ctx)
        assert vr.checks_run == 2
