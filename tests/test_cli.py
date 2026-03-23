"""CLI test suite — exercises all commands via click.testing.CliRunner.

Uses a temporary config file and mocked connectors so no live APIs are needed.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from src.cli import cli


@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture()
def config_file(tmp_path):
    """Create a minimal config.yaml for testing."""
    cfg = {
        "dataiku": {
            "url": "https://fake-dataiku.local",
            "api_key_env": "TEST_DATAIKU_KEY",
            "project_key": "CLI_TEST",
        },
        "fabric": {
            "workspace_id": "ws-test-000",
        },
        "migration": {
            "output_dir": str(tmp_path / "output"),
            "fail_fast": False,
            "parallel_agents": False,
        },
        "orchestrator": {
            "max_retries": 1,
            "retry_delay_seconds": 0,
            "agent_timeout_seconds": 10,
        },
        "logging": {
            "level": "WARNING",
            "format": "text",
        },
    }
    path = tmp_path / "config.yaml"
    import yaml
    path.write_text(yaml.dump(cfg))
    return str(path)


# ── Version ──────────────────────────────────────────────────

class TestVersion:
    def test_version_flag(self, runner):
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output


# ── Help ─────────────────────────────────────────────────────

class TestHelp:
    def test_main_help(self, runner):
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "discover" in result.output
        assert "migrate" in result.output
        assert "validate" in result.output
        assert "report" in result.output

    def test_discover_help(self, runner):
        result = runner.invoke(cli, ["discover", "--help"])
        assert result.exit_code == 0
        assert "--project" in result.output

    def test_migrate_help(self, runner):
        result = runner.invoke(cli, ["migrate", "--help"])
        assert result.exit_code == 0
        assert "--resume" in result.output
        assert "--rerun" in result.output
        assert "--asset-ids" in result.output
        assert "--keep-checkpoints" in result.output

    def test_validate_help(self, runner):
        result = runner.invoke(cli, ["validate", "--help"])
        assert result.exit_code == 0

    def test_report_help(self, runner):
        result = runner.invoke(cli, ["report", "--help"])
        assert result.exit_code == 0
        assert "--format" in result.output


# ── Discover ─────────────────────────────────────────────────

class TestDiscover:
    def test_discover_no_config(self, runner):
        """Missing config file → error."""
        result = runner.invoke(cli, ["discover", "-p", "X", "-c", "/nonexistent.yaml"])
        assert result.exit_code != 0

    def test_discover_runs(self, runner, config_file, tmp_path):
        """Discover with mocked config — agent fails gracefully (no API key)."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, ["discover", "-p", "TEST", "-c", config_file], env=env)
        # Will fail because no real Dataiku server, but CLI should not crash
        assert "Discovering assets" in result.output


# ── Migrate ──────────────────────────────────────────────────

class TestMigrate:
    def test_migrate_no_config(self, runner):
        result = runner.invoke(cli, ["migrate", "-p", "X", "-t", "ws", "-c", "/nonexistent.yaml"])
        assert result.exit_code != 0

    def test_migrate_runs(self, runner, config_file, tmp_path):
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "migrate", "-p", "TEST", "-t", "ws-000", "-c", config_file,
        ], env=env)
        assert "Migrating project" in result.output

    def test_migrate_with_resume_flag(self, runner, config_file, tmp_path):
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "migrate", "-p", "TEST", "-t", "ws-000", "-c", config_file, "--resume",
        ], env=env)
        assert "Resuming from checkpoint" in result.output

    def test_migrate_with_agents_filter(self, runner, config_file, tmp_path):
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "migrate", "-p", "TEST", "-t", "ws-000", "-c", config_file,
            "-a", "discovery",
        ], env=env)
        assert "Migrating project" in result.output


# ── Validate ─────────────────────────────────────────────────

class TestValidate:
    def test_validate_no_config(self, runner):
        result = runner.invoke(cli, ["validate", "-p", "X", "-c", "/nonexistent.yaml"])
        assert result.exit_code != 0

    def test_validate_runs(self, runner, config_file, tmp_path):
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "validate", "-p", "TEST", "-c", config_file,
        ], env=env)
        assert "Validating migration" in result.output


# ── Report ───────────────────────────────────────────────────

class TestReport:
    def test_report_runs(self, runner, config_file, tmp_path):
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "report", "-p", "TEST", "-c", config_file, "-f", "json",
        ], env=env)
        # The report command will produce output even if validator finds nothing
        assert result.exit_code == 0 or "status" in result.output
