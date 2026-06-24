"""Integration tests for Wave A operational runtime wiring.

Covers:
- Orchestrator snapshot + deployment manifest lifecycle
- Idempotency behavior across consecutive runs
- Secret policy enforcement in config validation/loading
- CLI rollback dry-run and apply flows
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent, ValidationResult
from src.cli import cli
from src.core.config import AppConfig, load_config, validate_config
from src.core.registry import AssetRegistry
from src.core.orchestrator import Orchestrator
from src.core.secrets import PolicyViolation
from src.core.snapshot import SnapshotStore, capture_snapshot
from src.models.asset import Asset, AssetType, MigrationState


class DiscoveryDummyAgent(BaseAgent):
    @property
    def name(self) -> str:
        return "discovery"

    @property
    def description(self) -> str:
        return "dummy discovery"

    async def execute(self, context):
        context.registry.add_asset(
            Asset(
                id="asset_sql_1",
                type=AssetType.RECIPE_SQL,
                name="orders_recipe",
                source_project=context.config.dataiku.project_key,
                state=MigrationState.CONVERTED,
                target_fabric_asset={"id": "fab_item_1"},
                metadata={"sql": "SELECT 1"},
            )
        )
        return AgentResult(
            agent_name=self.name,
            status=AgentStatus.COMPLETED,
            assets_processed=1,
            assets_converted=1,
        )

    async def validate(self, context):
        return ValidationResult(passed=True, checks_run=1, checks_passed=1)


def _make_config(tmp_path: Path) -> AppConfig:
    return AppConfig.model_validate(
        {
            "dataiku": {
                "url": "https://example.local",
                "api_key_env": "TEST_KEY",
                "project_key": "PROJ",
            },
            "fabric": {
                "workspace_id": "ws-001",
            },
            "migration": {
                "output_dir": str(tmp_path / "output"),
                "parallel_agents": False,
                "fail_fast": False,
            },
            "orchestrator": {
                "max_retries": 1,
                "retry_delay_seconds": 0,
                "agent_timeout_seconds": 30,
            },
            "logging": {
                "level": "WARNING",
                "format": "text",
            },
            "deployment": {
                "enable_idempotency": True,
                "manifest_dir": str(tmp_path / "output" / "manifests"),
                "enable_snapshots": True,
                "snapshot_dir": str(tmp_path / "output" / "snapshots"),
                "enforce_secret_policy": True,
            },
        }
    )


def _make_config_yaml(tmp_path: Path) -> Path:
    cfg = {
        "dataiku": {
            "url": "https://example.local",
            "api_key_env": "TEST_KEY",
            "project_key": "PROJ",
        },
        "fabric": {
            "workspace_id": "ws-001",
        },
        "migration": {
            "output_dir": str(tmp_path / "output"),
            "parallel_agents": False,
            "fail_fast": False,
        },
        "orchestrator": {
            "max_retries": 1,
            "retry_delay_seconds": 0,
            "agent_timeout_seconds": 30,
        },
        "logging": {
            "level": "WARNING",
            "format": "text",
        },
        "deployment": {
            "enable_idempotency": True,
            "manifest_dir": str(tmp_path / "output" / "manifests"),
            "enable_snapshots": True,
            "snapshot_dir": str(tmp_path / "output" / "snapshots"),
            "enforce_secret_policy": True,
        },
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return path


class TestOrchestratorOperationalArtifacts:
    def test_run_pipeline_writes_snapshot_and_manifest(self, tmp_path):
        cfg = _make_config(tmp_path)
        registry = AssetRegistry(project_key="PROJ", registry_path=tmp_path / "output" / "registry.json")
        orch = Orchestrator(cfg, registry)
        orch.register_agent(DiscoveryDummyAgent())

        asyncio.run(orch.run_pipeline(agent_names=["discovery"]))

        snapshots = list((tmp_path / "output" / "snapshots").glob("snapshot_*.json"))
        manifests = list((tmp_path / "output" / "manifests").glob("deployment_*.json"))

        assert len(snapshots) == 1
        assert len(manifests) == 1
        assert (tmp_path / "output" / "manifests" / "latest.json").exists()

        latest = json.loads((tmp_path / "output" / "manifests" / "latest.json").read_text(encoding="utf-8"))
        assert latest["total_count"] == 1
        assert latest["deployed_count"] == 1
        assert latest["skipped_count"] == 0

    def test_second_run_marks_unchanged_asset_as_skipped(self, tmp_path):
        cfg = _make_config(tmp_path)
        registry = AssetRegistry(project_key="PROJ", registry_path=tmp_path / "output" / "registry.json")
        orch = Orchestrator(cfg, registry)
        orch.register_agent(DiscoveryDummyAgent())

        asyncio.run(orch.run_pipeline(agent_names=["discovery"]))
        # keep registry state for second run to simulate operational continuity
        asyncio.run(orch.run_pipeline(agent_names=["discovery"]))

        latest = json.loads((tmp_path / "output" / "manifests" / "latest.json").read_text(encoding="utf-8"))
        assert latest["total_count"] >= 1
        assert latest["skipped_count"] >= 1

    def test_status_exposes_last_snapshot_and_manifest(self, tmp_path):
        cfg = _make_config(tmp_path)
        registry = AssetRegistry(project_key="PROJ", registry_path=tmp_path / "output" / "registry.json")
        orch = Orchestrator(cfg, registry)
        orch.register_agent(DiscoveryDummyAgent())

        asyncio.run(orch.run_pipeline(agent_names=["discovery"]))
        status = orch.get_status()

        assert status["last_snapshot"] is not None
        assert status["last_manifest"] is not None


class TestSecretPolicyIntegration:
    def test_validate_config_flags_plain_secrets_as_errors(self, tmp_path):
        cfg = {
            "dataiku": {
                "url": "https://example.local",
                "project_key": "P",
                "api_key_env": "DATAIKU_API_KEY",
                "api_key": "hardcoded-secret",  # forbidden
            },
            "fabric": {
                "workspace_id": "ws-001",
            },
        }
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.safe_dump(cfg), encoding="utf-8")

        issues = validate_config(path)
        errors = [i for i in issues if i["level"] == "error"]
        assert any("Secret policy violation" in e["message"] for e in errors)

    def test_load_config_raises_policy_violation_for_plain_secrets(self, tmp_path):
        cfg = {
            "dataiku": {
                "url": "https://example.local",
                "project_key": "P",
                "api_key_env": "DATAIKU_API_KEY",
                "api_key": "hardcoded-secret",  # forbidden
            },
            "fabric": {
                "workspace_id": "ws-001",
            },
        }
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.safe_dump(cfg), encoding="utf-8")

        with pytest.raises(PolicyViolation):
            load_config(path)


class TestRollbackCLI:
    def test_rollback_dry_run_outputs_summary(self, tmp_path):
        config_path = _make_config_yaml(tmp_path)

        # Prepare snapshot data
        registry = AssetRegistry(project_key="PROJ", registry_path=tmp_path / "output" / "registry.json")
        registry.add_asset(
            Asset(
                id="asset_sql_1",
                type=AssetType.RECIPE_SQL,
                name="orders_recipe",
                source_project="PROJ",
                state=MigrationState.CONVERTED,
            )
        )
        store = SnapshotStore(tmp_path / "output" / "snapshots")
        snap = capture_snapshot("snap-001", "PROJ", registry)
        store.save(snap)
        registry.save()

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "rollback",
                "-c",
                str(config_path),
                "--snapshot-id",
                "snap-001",
                "--dry-run",
                "-f",
                "json",
            ],
            env={"TEST_KEY": "dummy"},
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["mode"] == "dry-run"
        assert payload["summary"]["snapshot_id"] == "snap-001"

    def test_rollback_apply_restores_registry_state(self, tmp_path):
        config_path = _make_config_yaml(tmp_path)

        registry = AssetRegistry(project_key="PROJ", registry_path=tmp_path / "output" / "registry.json")
        original = Asset(
            id="asset_sql_1",
            type=AssetType.RECIPE_SQL,
            name="orders_recipe",
            source_project="PROJ",
            state=MigrationState.CONVERTED,
        )
        registry.add_asset(original)

        store = SnapshotStore(tmp_path / "output" / "snapshots")
        store.save(capture_snapshot("snap-apply", "PROJ", registry))

        # mutate registry to deployed state and persist
        registry.add_asset(
            Asset(
                id="asset_sql_1",
                type=AssetType.RECIPE_SQL,
                name="orders_recipe",
                source_project="PROJ",
                state=MigrationState.DEPLOYED,
            )
        )
        registry.save()

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "rollback",
                "-c",
                str(config_path),
                "--snapshot-id",
                "snap-apply",
                "-f",
                "json",
            ],
            env={"TEST_KEY": "dummy"},
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["mode"] == "apply"

        reloaded = AssetRegistry(project_key="PROJ", registry_path=tmp_path / "output" / "registry.json")
        reloaded.load()
        assert reloaded.get_asset("asset_sql_1").state == MigrationState.CONVERTED
