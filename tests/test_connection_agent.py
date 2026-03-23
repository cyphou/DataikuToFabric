"""Tests for the connection mapper agent — Phase 5."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from src.agents.connection_agent import (
    CONNECTION_MAP,
    ConnectionMapperAgent,
    _build_gateway_template,
    _build_pipeline_template,
    _build_shortcut_template,
    _get_required_secrets,
    build_connection_config,
)
from src.agents.base_agent import AgentStatus
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


def _make_conn_asset(
    name: str,
    conn_type: str,
    params: dict | None = None,
) -> Asset:
    metadata = {"type": conn_type, "params": params or {}}
    return Asset(
        id=f"conn_{name}",
        type=AssetType.CONNECTION,
        name=name,
        source_project="TEST",
        state=MigrationState.DISCOVERED,
        metadata=metadata,
    )


# ═══════════════════════════════════════════════════════════════
# 1. Connection map coverage
# ═══════════════════════════════════════════════════════════════

class TestConnectionMap:
    """Test that all connection types have valid mappings."""

    def test_all_known_types_present(self):
        expected = {
            "oracle", "postgresql", "sqlserver", "mysql", "s3", "azure_blob",
            "azure_datalake", "hdfs", "bigquery", "snowflake", "filesystem", "ftp",
        }
        assert expected == set(CONNECTION_MAP.keys())

    def test_all_mappings_have_fabric_type(self):
        for name, mapping in CONNECTION_MAP.items():
            assert "fabric_type" in mapping, f"{name} missing fabric_type"

    def test_all_mappings_have_notes(self):
        for name, mapping in CONNECTION_MAP.items():
            assert "notes" in mapping, f"{name} missing notes"

    def test_all_mappings_have_category(self):
        for name, mapping in CONNECTION_MAP.items():
            assert "category" in mapping, f"{name} missing category"

    def test_database_types_use_gateway_or_direct(self):
        db_types = ["oracle", "postgresql", "mysql"]
        for t in db_types:
            assert "Gateway" in CONNECTION_MAP[t]["fabric_type"]

    def test_cloud_storage_uses_shortcuts(self):
        assert "Shortcut" in CONNECTION_MAP["s3"]["fabric_type"]
        assert "Shortcut" in CONNECTION_MAP["azure_blob"]["fabric_type"]
        assert "Shortcut" in CONNECTION_MAP["azure_datalake"]["fabric_type"]


# ═══════════════════════════════════════════════════════════════
# 2. Gateway template tests
# ═══════════════════════════════════════════════════════════════

class TestGatewayTemplate:
    """Test _build_gateway_template() for database connections."""

    def test_oracle_template(self):
        tmpl = _build_gateway_template("oracle", {"host": "db.example.com", "port": "1521", "database": "ORCL"})
        assert "db.example.com" in tmpl["connection_string"]
        assert "1521" in tmpl["connection_string"]
        assert tmpl["auth_type"] == "Basic"
        assert "username" in tmpl["required_secrets"]

    def test_postgresql_template(self):
        tmpl = _build_gateway_template("postgresql", {"host": "pg.local", "port": "5432", "database": "mydb"})
        assert "Host=pg.local" in tmpl["connection_string"]
        assert "Port=5432" in tmpl["connection_string"]

    def test_mysql_template(self):
        tmpl = _build_gateway_template("mysql", {"host": "mysql.local", "port": "3306", "database": "app"})
        assert "Server=mysql.local" in tmpl["connection_string"]

    def test_sqlserver_windows_auth(self):
        tmpl = _build_gateway_template("sqlserver", {"host": "sql.local", "port": "1433", "database": "prod"})
        assert tmpl["auth_type"] == "Windows"

    def test_unknown_type_fallback(self):
        tmpl = _build_gateway_template("exotic_db", {"host": "h", "port": "123", "database": "d"})
        assert "exotic_db" in tmpl["connection_string"]
        assert tmpl["auth_type"] == "Basic"

    def test_missing_params_uses_placeholders(self):
        tmpl = _build_gateway_template("oracle", {})
        assert "<HOST>" in tmpl["connection_string"]
        assert "<PORT>" in tmpl["connection_string"]


# ═══════════════════════════════════════════════════════════════
# 3. Shortcut template tests
# ═══════════════════════════════════════════════════════════════

class TestShortcutTemplate:
    """Test _build_shortcut_template() for cloud storage connections."""

    def test_s3_shortcut(self):
        tmpl = _build_shortcut_template("s3", {"bucket": "my-bucket", "path": "/data/"})
        assert tmpl["shortcut_type"] == "AmazonS3"
        assert tmpl["location"] == "my-bucket"
        assert tmpl["sub_path"] == "/data/"
        assert "aws_access_key_id" in tmpl["required_secrets"]
        assert len(tmpl["steps"]) >= 2

    def test_adls_shortcut(self):
        tmpl = _build_shortcut_template("azure_blob", {
            "account": "mystorageacct", "container": "raw"
        })
        assert tmpl["shortcut_type"] == "ADLS Gen2"
        assert "mystorageacct" in tmpl["location"]
        assert "raw" in tmpl["location"]
        assert tmpl["required_secrets"] == []  # Managed identity

    def test_azure_datalake_shortcut(self):
        tmpl = _build_shortcut_template("azure_datalake", {"account": "dlake", "container": "c"})
        assert tmpl["shortcut_type"] == "ADLS Gen2"

    def test_unknown_returns_empty(self):
        tmpl = _build_shortcut_template("exotic", {})
        assert tmpl == {}

    def test_s3_missing_bucket_uses_placeholder(self):
        tmpl = _build_shortcut_template("s3", {})
        assert "<S3_BUCKET>" in tmpl["location"]


# ═══════════════════════════════════════════════════════════════
# 4. Pipeline template tests
# ═══════════════════════════════════════════════════════════════

class TestPipelineTemplate:
    """Test _build_pipeline_template() for external data sources."""

    def test_bigquery_pipeline(self):
        tmpl = _build_pipeline_template("bigquery", {"host": "bq.google.com", "database": "project1"})
        assert tmpl["activity_type"] == "Copy"
        assert tmpl["source_type"] == "bigquery"
        assert tmpl["sink_type"] == "Lakehouse"
        assert len(tmpl["steps"]) >= 3

    def test_snowflake_pipeline(self):
        tmpl = _build_pipeline_template("snowflake", {})
        assert tmpl["source_type"] == "snowflake"
        assert "account" in tmpl["required_secrets"]


# ═══════════════════════════════════════════════════════════════
# 5. Required secrets tests
# ═══════════════════════════════════════════════════════════════

class TestRequiredSecrets:
    """Test _get_required_secrets() for various connection types."""

    def test_database_secrets(self):
        for t in ["oracle", "postgresql", "mysql"]:
            secrets = _get_required_secrets(t)
            assert "username" in secrets
            assert "password" in secrets

    def test_s3_secrets(self):
        secrets = _get_required_secrets("s3")
        assert "aws_access_key_id" in secrets

    def test_azure_no_secrets(self):
        # Azure uses managed identity
        assert _get_required_secrets("azure_blob") == []

    def test_bigquery_service_account(self):
        secrets = _get_required_secrets("bigquery")
        assert "service_account_json" in secrets

    def test_snowflake_secrets(self):
        secrets = _get_required_secrets("snowflake")
        assert "account" in secrets


# ═══════════════════════════════════════════════════════════════
# 6. build_connection_config() tests
# ═══════════════════════════════════════════════════════════════

class TestBuildConnectionConfig:
    """Test the overall build_connection_config() function."""

    def test_gateway_connection_has_gateway_config(self):
        mapping = CONNECTION_MAP["oracle"]
        cfg = build_connection_config("c1", "oracle", mapping, "ws-001",
                                      {"host": "db.co", "port": "1521", "database": "PROD"})
        assert "gateway_config" in cfg
        assert cfg["fabric_type"] == "OnPremisesDataGateway"
        assert len(cfg["manual_steps"]) >= 3

    def test_shortcut_connection_has_shortcut_config(self):
        mapping = CONNECTION_MAP["s3"]
        cfg = build_connection_config("c2", "s3", mapping, "ws-001",
                                      {"bucket": "data-lake"})
        assert "shortcut_config" in cfg
        assert cfg["shortcut_config"]["shortcut_type"] == "AmazonS3"

    def test_pipeline_connection_has_pipeline_config(self):
        mapping = CONNECTION_MAP["bigquery"]
        cfg = build_connection_config("c3", "bigquery", mapping, "ws-001")
        assert "pipeline_config" in cfg

    def test_filesystem_has_upload_steps(self):
        mapping = CONNECTION_MAP["filesystem"]
        cfg = build_connection_config("c4", "filesystem", mapping, "ws-001")
        assert any("Upload" in s for s in cfg["manual_steps"])

    def test_direct_connection_steps(self):
        mapping = CONNECTION_MAP["sqlserver"]
        cfg = build_connection_config("c5", "sqlserver", mapping, "ws-001")
        assert any("Verify" in s or "Configure" in s for s in cfg["manual_steps"])

    def test_hdfs_file_copy_steps(self):
        mapping = CONNECTION_MAP["hdfs"]
        cfg = build_connection_config("c6", "hdfs", mapping, "ws-001")
        assert any("HDFS" in s or "Export" in s for s in cfg["manual_steps"])

    def test_config_has_workspace_id(self):
        mapping = CONNECTION_MAP["oracle"]
        cfg = build_connection_config("c7", "oracle", mapping, "my-ws-id")
        assert cfg["workspace_id"] == "my-ws-id"


# ═══════════════════════════════════════════════════════════════
# 7. Agent execute tests
# ═══════════════════════════════════════════════════════════════

class TestConnectionAgentExecute:
    """Test ConnectionMapperAgent.execute()."""

    @pytest.fixture
    def agent(self):
        return ConnectionMapperAgent()

    @pytest.fixture
    def ctx(self, tmp_path):
        reg = AssetRegistry("TEST", registry_path=tmp_path / "reg.json")
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        return StubContext(registry=reg, config=cfg)

    def test_converts_oracle_connection(self, agent, ctx):
        asset = _make_conn_asset("ora_prod", "oracle", {"host": "ora.host", "port": "1521", "database": "PROD"})
        ctx.registry.add_asset(asset)

        result = asyncio.run(agent.execute(ctx))

        assert result.status == AgentStatus.COMPLETED
        assert result.assets_converted == 1
        assert result.assets_failed == 0

    def test_creates_config_file(self, agent, ctx):
        asset = _make_conn_asset("pg_conn", "postgresql")
        ctx.registry.add_asset(asset)

        asyncio.run(agent.execute(ctx))

        config_file = Path(ctx.config.migration.output_dir) / "connections" / "pg_conn.json"
        assert config_file.exists()
        data = json.loads(config_file.read_text())
        assert data["fabric_type"] == "OnPremisesDataGateway"
        assert "gateway_config" in data

    def test_s3_creates_shortcut_config(self, agent, ctx):
        asset = _make_conn_asset("s3_data", "s3", {"bucket": "my-bucket"})
        ctx.registry.add_asset(asset)

        asyncio.run(agent.execute(ctx))

        config_file = Path(ctx.config.migration.output_dir) / "connections" / "s3_data.json"
        data = json.loads(config_file.read_text())
        assert "shortcut_config" in data
        assert data["shortcut_config"]["location"] == "my-bucket"

    def test_unknown_type_flags_review(self, agent, ctx):
        asset = _make_conn_asset("exotic", "teradata")
        ctx.registry.add_asset(asset)

        result = asyncio.run(agent.execute(ctx))

        assert result.assets_failed == 1
        assert len(result.review_flags) == 1
        assert "Unknown connection type" in result.review_flags[0]

    def test_multiple_connections(self, agent, ctx):
        for name, ctype in [("a", "oracle"), ("b", "s3"), ("c", "bigquery")]:
            ctx.registry.add_asset(_make_conn_asset(name, ctype))

        result = asyncio.run(agent.execute(ctx))

        assert result.assets_processed == 3
        assert result.assets_converted == 3

    def test_sets_registry_target(self, agent, ctx):
        asset = _make_conn_asset("azure_stor", "azure_blob")
        ctx.registry.add_asset(asset)

        asyncio.run(agent.execute(ctx))

        updated = ctx.registry.get_asset("conn_azure_stor")
        assert updated.state == MigrationState.CONVERTED
        assert updated.target_fabric_asset["fabric_type"] == "OneLakeShortcut_ADLS"
        assert updated.target_fabric_asset["category"] == "cloud_storage"


# ═══════════════════════════════════════════════════════════════
# 8. Agent validate tests
# ═══════════════════════════════════════════════════════════════

class TestConnectionAgentValidate:
    """Test ConnectionMapperAgent.validate()."""

    @pytest.fixture
    def agent(self):
        return ConnectionMapperAgent()

    @pytest.fixture
    def ctx(self, tmp_path):
        reg = AssetRegistry("TEST", registry_path=tmp_path / "reg.json")
        cfg = StubConfig(migration=StubMigrationConfig(output_dir=str(tmp_path / "out")))
        return StubContext(registry=reg, config=cfg)

    def test_passes_after_execute(self, agent, ctx):
        asset = _make_conn_asset("valid", "oracle")
        ctx.registry.add_asset(asset)

        asyncio.run(agent.execute(ctx))
        result = asyncio.run(agent.validate(ctx))

        assert result.passed is True
        assert result.checks_run >= 2  # File exists + valid JSON

    def test_fails_missing_file(self, agent, ctx):
        asset = _make_conn_asset("broken", "oracle")
        ctx.registry.add_asset(asset)
        ctx.registry.update_state("conn_broken", MigrationState.CONVERTED)
        ctx.registry.set_target("conn_broken", {
            "path": "/nonexistent/file.json",
            "fabric_type": "OnPremisesDataGateway",
        })

        result = asyncio.run(agent.validate(ctx))

        assert result.passed is False
        assert any("Connection config not found" in f for f in result.failures)

    def test_fails_invalid_json(self, agent, ctx, tmp_path):
        asset = _make_conn_asset("badjson", "oracle")
        ctx.registry.add_asset(asset)
        ctx.registry.update_state("conn_badjson", MigrationState.CONVERTED)

        bad_file = tmp_path / "bad.json"
        bad_file.write_text("not json at all", encoding="utf-8")
        ctx.registry.set_target("conn_badjson", {
            "path": str(bad_file),
            "fabric_type": "OnPremisesDataGateway",
        })

        result = asyncio.run(agent.validate(ctx))

        assert result.passed is False
        assert any("not valid JSON" in f for f in result.failures)

    def test_no_connections_passes(self, agent, ctx):
        result = asyncio.run(agent.validate(ctx))
        assert result.passed is True
        assert result.checks_run == 0
