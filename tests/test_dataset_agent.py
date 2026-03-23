"""Tests for the dataset migration agent — Phase 5."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from src.agents.dataset_agent import (
    DatasetMigrationAgent,
    LAKEHOUSE_TYPE_MAP,
    WAREHOUSE_TYPE_MAP,
    _map_column_type,
    compare_schemas,
    decide_storage,
    generate_ddl,
    generate_export_manifest,
)
from src.agents.base_agent import AgentStatus, AgentResult
from src.core.registry import AssetRegistry
from src.models.asset import Asset, AssetType, MigrationState


# ── Helpers ───────────────────────────────────────────────────

@dataclass
class StubDataikuConfig:
    url: str = "https://fake.com"
    api_key_env: str = "DATAIKU_API_KEY"
    project_key: str = "TEST"


@dataclass
class StubFabricConfig:
    workspace_id: str = "00000000-0000-0000-0000-000000000000"
    lakehouse_name: str = "lh_test"
    warehouse_name: str = "wh_test"


@dataclass
class StubMigrationConfig:
    output_dir: str = "./output"
    default_storage: str = "lakehouse"
    target_sql_dialect: str = "tsql"


@dataclass
class StubConfig:
    dataiku: Any = None
    fabric: Any = None
    migration: Any = None

    def __post_init__(self):
        self.dataiku = self.dataiku or StubDataikuConfig()
        self.fabric = self.fabric or StubFabricConfig()
        self.migration = self.migration or StubMigrationConfig()


@dataclass
class StubContext:
    registry: Any = None
    config: Any = None


def _make_dataset_asset(
    name: str,
    columns: list[dict] | None = None,
    ds_type: str = "filesystem",
    extra_metadata: dict | None = None,
) -> Asset:
    metadata = {"type": ds_type, "schema": {"columns": columns or []}}
    if extra_metadata:
        metadata.update(extra_metadata)
    return Asset(
        id=f"ds_{name}",
        type=AssetType.DATASET,
        name=name,
        source_project="TEST",
        state=MigrationState.DISCOVERED,
        metadata=metadata,
    )


SAMPLE_COLUMNS = [
    {"name": "id", "type": "int", "nullable": False},
    {"name": "name", "type": "string", "nullable": True},
    {"name": "created_at", "type": "timestamp", "nullable": True},
    {"name": "amount", "type": "double", "nullable": True},
    {"name": "active", "type": "boolean", "nullable": False},
]


# ═══════════════════════════════════════════════════════════════
# 1. Storage decision tests
# ═══════════════════════════════════════════════════════════════

class TestDecideStorage:
    """Test decide_storage() logic."""

    def test_sql_table_goes_to_warehouse(self):
        assert decide_storage({"type": "sql_table"}) == "warehouse"

    def test_oracle_goes_to_warehouse(self):
        assert decide_storage({"type": "oracle"}) == "warehouse"

    def test_postgresql_goes_to_warehouse(self):
        assert decide_storage({"type": "postgresql"}) == "warehouse"

    def test_filesystem_goes_to_lakehouse(self):
        assert decide_storage({"type": "filesystem"}) == "lakehouse"

    def test_s3_goes_to_lakehouse(self):
        assert decide_storage({"type": "s3"}) == "lakehouse"

    def test_hdfs_goes_to_lakehouse(self):
        assert decide_storage({"type": "hdfs"}) == "lakehouse"

    def test_azure_blob_goes_to_lakehouse(self):
        assert decide_storage({"type": "azure_blob"}) == "lakehouse"

    def test_azure_datalake_goes_to_lakehouse(self):
        assert decide_storage({"type": "azure_datalake"}) == "lakehouse"

    def test_unknown_returns_default(self):
        assert decide_storage({"type": "exotic"}) == "lakehouse"

    def test_unknown_custom_default(self):
        assert decide_storage({"type": "exotic"}, default="warehouse") == "warehouse"

    def test_explicit_storage_override(self):
        assert decide_storage({"type": "filesystem", "storage": "warehouse"}) == "warehouse"

    def test_connection_type_secondary_signal(self):
        meta = {"type": "", "params": {"connection": "oracle"}}
        assert decide_storage(meta) == "warehouse"


# ═══════════════════════════════════════════════════════════════
# 2. Type mapping tests
# ═══════════════════════════════════════════════════════════════

class TestColumnTypeMapping:
    """Test _map_column_type() with precision and special types."""

    def test_basic_int_lakehouse(self):
        assert _map_column_type({"type": "int"}, "lakehouse") == "INT"

    def test_basic_string_warehouse(self):
        assert _map_column_type({"type": "string"}, "warehouse") == "NVARCHAR(MAX)"

    def test_decimal_with_precision(self):
        assert _map_column_type({"type": "decimal(18,2)"}, "warehouse") == "DECIMAL(18,2)"

    def test_decimal_with_precision_lakehouse(self):
        assert _map_column_type({"type": "decimal(10,4)"}, "lakehouse") == "DECIMAL(10,4)"

    def test_string_with_length_warehouse(self):
        assert _map_column_type({"type": "string(255)"}, "warehouse") == "NVARCHAR(255)"

    def test_string_with_maxlength_hint(self):
        col = {"type": "string", "maxLength": 100}
        assert _map_column_type(col, "warehouse") == "NVARCHAR(100)"

    def test_boolean_lakehouse(self):
        assert _map_column_type({"type": "boolean"}, "lakehouse") == "BOOLEAN"

    def test_boolean_warehouse(self):
        assert _map_column_type({"type": "boolean"}, "warehouse") == "BIT"

    def test_timestamp_lakehouse(self):
        assert _map_column_type({"type": "timestamp"}, "lakehouse") == "TIMESTAMP"

    def test_timestamp_warehouse(self):
        assert _map_column_type({"type": "timestamp"}, "warehouse") == "DATETIME2"

    def test_unknown_type_falls_back_to_string(self):
        assert _map_column_type({"type": "exotic"}, "lakehouse") == "STRING"
        assert _map_column_type({"type": "exotic"}, "warehouse") == "NVARCHAR(MAX)"

    def test_tinyint_smallint(self):
        assert _map_column_type({"type": "tinyint"}, "warehouse") == "TINYINT"
        assert _map_column_type({"type": "smallint"}, "lakehouse") == "SMALLINT"

    def test_binary(self):
        assert _map_column_type({"type": "binary"}, "lakehouse") == "BINARY"
        assert _map_column_type({"type": "binary"}, "warehouse") == "VARBINARY(MAX)"


# ═══════════════════════════════════════════════════════════════
# 3. DDL generation tests
# ═══════════════════════════════════════════════════════════════

class TestGenerateDDL:
    """Test generate_ddl() for both targets."""

    def test_lakehouse_basic(self):
        ddl = generate_ddl("my_table", SAMPLE_COLUMNS, target="lakehouse")
        assert "CREATE TABLE IF NOT EXISTS my_table" in ddl
        assert "USING DELTA" in ddl
        assert "[id] INT NOT NULL" in ddl
        assert "[name] STRING NULL" in ddl

    def test_warehouse_basic(self):
        ddl = generate_ddl("my_table", SAMPLE_COLUMNS, target="warehouse")
        assert "CREATE TABLE [my_table]" in ddl
        assert "USING DELTA" not in ddl
        assert "[id] INT NOT NULL" in ddl
        assert "[active] BIT NOT NULL" in ddl

    def test_empty_columns(self):
        ddl = generate_ddl("empty_table", [], target="lakehouse")
        assert "CREATE TABLE IF NOT EXISTS empty_table" in ddl

    def test_partition_by_lakehouse(self):
        ddl = generate_ddl("partitioned", SAMPLE_COLUMNS, target="lakehouse",
                           partition_by=["created_at"])
        assert "PARTITIONED BY (created_at)" in ddl

    def test_partition_by_multiple(self):
        ddl = generate_ddl("multi_part", SAMPLE_COLUMNS, target="lakehouse",
                           partition_by=["active", "created_at"])
        assert "PARTITIONED BY (active, created_at)" in ddl

    def test_nullable_default_true(self):
        cols = [{"name": "x", "type": "string"}]
        ddl = generate_ddl("t", cols, target="warehouse")
        assert "[x] NVARCHAR(MAX) NULL" in ddl

    def test_precision_in_ddl(self):
        cols = [{"name": "price", "type": "decimal(18,2)", "nullable": False}]
        ddl = generate_ddl("prices", cols, target="warehouse")
        assert "[price] DECIMAL(18,2) NOT NULL" in ddl


# ═══════════════════════════════════════════════════════════════
# 4. Schema comparison tests
# ═══════════════════════════════════════════════════════════════

class TestCompareSchemas:
    """Test compare_schemas() utility."""

    def test_identical_schemas(self):
        cols = [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}]
        diffs = compare_schemas(cols, cols)
        assert diffs == []

    def test_missing_column_in_target(self):
        src = [{"name": "id", "type": "int"}, {"name": "extra", "type": "string"}]
        tgt = [{"name": "id", "type": "int"}]
        diffs = compare_schemas(src, tgt)
        assert any("extra" in d and "missing from target" in d for d in diffs)

    def test_extra_column_in_target(self):
        src = [{"name": "id", "type": "int"}]
        tgt = [{"name": "id", "type": "int"}, {"name": "bonus", "type": "float"}]
        diffs = compare_schemas(src, tgt)
        assert any("bonus" in d and "missing from source" in d for d in diffs)

    def test_type_mismatch(self):
        src = [{"name": "val", "type": "int"}]
        tgt = [{"name": "val", "type": "string"}]
        diffs = compare_schemas(src, tgt)
        assert any("type mismatch" in d for d in diffs)

    def test_case_insensitive_comparison(self):
        src = [{"name": "ID", "type": "int"}]
        tgt = [{"name": "id", "type": "int"}]
        diffs = compare_schemas(src, tgt)
        assert diffs == []

    def test_multiple_differences(self):
        src = [{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]
        tgt = [{"name": "a", "type": "float"}, {"name": "c", "type": "string"}]
        diffs = compare_schemas(src, tgt)
        assert len(diffs) >= 2  # type mismatch on a, missing b, extra c


# ═══════════════════════════════════════════════════════════════
# 5. Export manifest tests
# ═══════════════════════════════════════════════════════════════

class TestExportManifest:
    """Test generate_export_manifest()."""

    def test_lakehouse_manifest(self):
        manifest = generate_export_manifest("sales", SAMPLE_COLUMNS, "lakehouse")
        assert manifest["asset_name"] == "sales"
        assert manifest["storage"] == "lakehouse"
        assert manifest["target_path"] == "Tables/sales"
        assert manifest["column_count"] == 5

    def test_warehouse_manifest(self):
        manifest = generate_export_manifest("orders", SAMPLE_COLUMNS, "warehouse")
        assert manifest["target_path"] == "orders"
        assert manifest["storage"] == "warehouse"

    def test_parquet_default_format(self):
        manifest = generate_export_manifest("t", [], "lakehouse")
        assert manifest["export_format"] == "parquet"


# ═══════════════════════════════════════════════════════════════
# 6. Agent execute tests
# ═══════════════════════════════════════════════════════════════

class TestDatasetAgentExecute:
    """Test DatasetMigrationAgent.execute()."""

    @pytest.fixture
    def agent(self):
        return DatasetMigrationAgent()

    @pytest.fixture
    def ctx(self, tmp_path):
        reg = AssetRegistry("TEST", registry_path=tmp_path / "reg.json")
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        return StubContext(registry=reg, config=cfg)

    def test_converts_single_dataset(self, agent, ctx):
        asset = _make_dataset_asset("customers", SAMPLE_COLUMNS)
        ctx.registry.add_asset(asset)

        result = asyncio.run(agent.execute(ctx))

        assert result.status == AgentStatus.COMPLETED
        assert result.assets_processed == 1
        assert result.assets_converted == 1
        assert result.assets_failed == 0

    def test_creates_ddl_file(self, agent, ctx):
        asset = _make_dataset_asset("orders", SAMPLE_COLUMNS, ds_type="oracle")
        ctx.registry.add_asset(asset)

        asyncio.run(agent.execute(ctx))

        ddl_file = Path(ctx.config.migration.output_dir) / "ddl" / "orders.sql"
        assert ddl_file.exists()
        content = ddl_file.read_text()
        assert "CREATE TABLE [orders]" in content

    def test_creates_manifest_file(self, agent, ctx):
        asset = _make_dataset_asset("logs", SAMPLE_COLUMNS, ds_type="s3")
        ctx.registry.add_asset(asset)

        asyncio.run(agent.execute(ctx))

        manifest = Path(ctx.config.migration.output_dir) / "manifests" / "logs.json"
        assert manifest.exists()
        data = json.loads(manifest.read_text())
        assert data["storage"] == "lakehouse"
        assert data["target_path"] == "Tables/logs"

    def test_sets_registry_target(self, agent, ctx):
        asset = _make_dataset_asset("products", SAMPLE_COLUMNS)
        ctx.registry.add_asset(asset)

        asyncio.run(agent.execute(ctx))

        updated = ctx.registry.get_asset("ds_products")
        assert updated.state == MigrationState.CONVERTED
        target = updated.target_fabric_asset
        assert target["storage"] == "lakehouse"
        assert target["column_count"] == 5

    def test_empty_schema_flags_review(self, agent, ctx):
        asset = _make_dataset_asset("empty_ds", [])
        ctx.registry.add_asset(asset)

        result = asyncio.run(agent.execute(ctx))

        assert result.assets_converted == 1
        assert len(result.review_flags) == 1
        assert "No columns" in result.review_flags[0]

    def test_multiple_datasets(self, agent, ctx):
        for name in ["a", "b", "c"]:
            ctx.registry.add_asset(_make_dataset_asset(name, SAMPLE_COLUMNS))

        result = asyncio.run(agent.execute(ctx))

        assert result.assets_processed == 3
        assert result.assets_converted == 3

    def test_warehouse_storage_for_sql_type(self, agent, ctx):
        asset = _make_dataset_asset("oracle_ds", SAMPLE_COLUMNS, ds_type="oracle")
        ctx.registry.add_asset(asset)

        asyncio.run(agent.execute(ctx))

        updated = ctx.registry.get_asset("ds_oracle_ds")
        assert updated.target_fabric_asset["storage"] == "warehouse"

    def test_partition_support(self, agent, ctx):
        asset = _make_dataset_asset("partitioned", SAMPLE_COLUMNS, extra_metadata={
            "partitioning": {"columns": ["created_at"]}
        })
        ctx.registry.add_asset(asset)

        asyncio.run(agent.execute(ctx))

        ddl_path = Path(ctx.config.migration.output_dir) / "ddl" / "partitioned.sql"
        assert "PARTITIONED BY (created_at)" in ddl_path.read_text()


# ═══════════════════════════════════════════════════════════════
# 7. Agent validate tests
# ═══════════════════════════════════════════════════════════════

class TestDatasetAgentValidate:
    """Test DatasetMigrationAgent.validate()."""

    @pytest.fixture
    def agent(self):
        return DatasetMigrationAgent()

    @pytest.fixture
    def ctx(self, tmp_path):
        reg = AssetRegistry("TEST", registry_path=tmp_path / "reg.json")
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        return StubContext(registry=reg, config=cfg)

    def test_validation_passes_after_execute(self, agent, ctx):
        asset = _make_dataset_asset("valid_ds", SAMPLE_COLUMNS)
        ctx.registry.add_asset(asset)

        asyncio.run(agent.execute(ctx))
        result = asyncio.run(agent.validate(ctx))

        assert result.passed is True
        assert result.checks_run >= 3  # DDL existence + CREATE TABLE + column count

    def test_validation_fails_missing_ddl(self, agent, ctx):
        asset = _make_dataset_asset("missing", SAMPLE_COLUMNS)
        ctx.registry.add_asset(asset)
        # Manually set state without actually creating files
        ctx.registry.update_state("ds_missing", MigrationState.CONVERTED)
        ctx.registry.set_target("ds_missing", {
            "ddl_path": "/nonexistent/path.sql",
            "column_count": 5,
        })

        result = asyncio.run(agent.validate(ctx))

        assert result.passed is False
        assert any("DDL file not found" in f for f in result.failures)

    def test_validation_checks_column_count(self, agent, ctx):
        asset = _make_dataset_asset("bad_cols", SAMPLE_COLUMNS)
        ctx.registry.add_asset(asset)

        # Execute first to create files, then tamper with registry
        asyncio.run(agent.execute(ctx))
        ctx.registry.set_target("ds_bad_cols", {
            **ctx.registry.get_asset("ds_bad_cols").target_fabric_asset,
            "column_count": 999,  # Wrong count
        })

        result = asyncio.run(agent.validate(ctx))

        assert result.passed is False
        assert any("Column count mismatch" in f for f in result.failures)

    def test_no_datasets_passes_validation(self, agent, ctx):
        result = asyncio.run(agent.validate(ctx))
        assert result.passed is True
        assert result.checks_run == 0
