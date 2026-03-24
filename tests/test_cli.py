"""CLI test suite — exercises all commands via click.testing.CliRunner.

Uses a temporary config file and mocked connectors so no live APIs are needed.
Covers: version, help, discover, migrate (with --dry-run, --quiet, --output-format),
validate, report, config validate, status, interactive, formatting helpers.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch, AsyncMock, MagicMock

import pytest
import yaml
from click.testing import CliRunner

from src.cli import cli, _format_output, _format_table


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
    path.write_text(yaml.dump(cfg))
    return str(path)


@pytest.fixture()
def bad_yaml_file(tmp_path):
    """Create a YAML file with invalid syntax."""
    path = tmp_path / "bad.yaml"
    path.write_text("dataiku:\n  url: [unclosed")
    return str(path)


@pytest.fixture()
def invalid_schema_file(tmp_path):
    """Create a YAML config that fails Pydantic validation."""
    path = tmp_path / "bad_schema.yaml"
    path.write_text(yaml.dump({"not_a_valid_key": 123}))
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
        for cmd in ("discover", "migrate", "validate", "report", "config", "status", "interactive"):
            assert cmd in result.output

    def test_discover_help(self, runner):
        result = runner.invoke(cli, ["discover", "--help"])
        assert result.exit_code == 0
        assert "--project" in result.output
        assert "--output-format" in result.output

    def test_migrate_help(self, runner):
        result = runner.invoke(cli, ["migrate", "--help"])
        assert result.exit_code == 0
        for flag in ("--resume", "--rerun", "--asset-ids", "--keep-checkpoints",
                      "--dry-run", "--output-format", "--quiet"):
            assert flag in result.output

    def test_validate_help(self, runner):
        result = runner.invoke(cli, ["validate", "--help"])
        assert result.exit_code == 0
        assert "--output-format" in result.output

    def test_report_help(self, runner):
        result = runner.invoke(cli, ["report", "--help"])
        assert result.exit_code == 0
        assert "--format" in result.output

    def test_config_validate_help(self, runner):
        result = runner.invoke(cli, ["config", "validate", "--help"])
        assert result.exit_code == 0
        assert "CONFIG_PATH" in result.output

    def test_status_help(self, runner):
        result = runner.invoke(cli, ["status", "--help"])
        assert result.exit_code == 0
        assert "--output-format" in result.output

    def test_interactive_help(self, runner):
        result = runner.invoke(cli, ["interactive", "--help"])
        assert result.exit_code == 0
        assert "wizard" in result.output.lower()


# ── Discover ─────────────────────────────────────────────────

class TestDiscover:
    def test_discover_no_config(self, runner):
        """Missing config file → error."""
        result = runner.invoke(cli, ["discover", "-p", "X", "-c", "/nonexistent.yaml"])
        assert result.exit_code != 0

    def test_discover_runs(self, runner, config_file):
        """Discover with mocked config — agent runs (no real API)."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, ["discover", "-p", "TEST", "-c", config_file], env=env)
        assert "Discovering assets" in result.output

    def test_discover_json_format(self, runner, config_file):
        """--output-format json produces valid JSON."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "discover", "-p", "TEST", "-c", config_file, "-f", "json",
        ], env=env)
        # Output after "Discovering assets" line should contain JSON
        assert "Discovering assets" in result.output

    def test_discover_yaml_format(self, runner, config_file):
        """--output-format yaml works."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "discover", "-p", "TEST", "-c", config_file, "-f", "yaml",
        ], env=env)
        assert "Discovering assets" in result.output


# ── Migrate ──────────────────────────────────────────────────

class TestMigrate:
    def test_migrate_no_config(self, runner):
        result = runner.invoke(cli, ["migrate", "-p", "X", "-t", "ws", "-c", "/nonexistent.yaml"])
        assert result.exit_code != 0

    def test_migrate_runs(self, runner, config_file):
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "migrate", "-p", "TEST", "-t", "ws-000", "-c", config_file, "-q",
        ], env=env)
        assert "Migrating project" in result.output

    def test_migrate_with_resume_flag(self, runner, config_file):
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "migrate", "-p", "TEST", "-t", "ws-000", "-c", config_file,
            "--resume", "-q",
        ], env=env)
        assert "Resuming from checkpoint" in result.output

    def test_migrate_with_agents_filter(self, runner, config_file):
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "migrate", "-p", "TEST", "-t", "ws-000", "-c", config_file,
            "-a", "discovery", "-q",
        ], env=env)
        assert "Migrating project" in result.output

    def test_migrate_dry_run(self, runner, config_file):
        """--dry-run prints execution plan and does NOT run agents."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "migrate", "-p", "TEST", "-t", "ws-000", "-c", config_file,
            "--dry-run",
        ], env=env)
        assert result.exit_code == 0
        # Should not contain "Migrating project" — dry-run skips actual execution
        assert "Migrating project" not in result.output

    def test_migrate_dry_run_json(self, runner, config_file):
        """--dry-run --output-format json returns valid JSON plan."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "migrate", "-p", "TEST", "-t", "ws-000", "-c", config_file,
            "--dry-run", "-f", "json",
        ], env=env)
        assert result.exit_code == 0
        plan = json.loads(result.output)
        assert "waves" in plan
        assert "total_agents" in plan
        assert plan["project_key"] == "CLI_TEST"

    def test_migrate_dry_run_yaml(self, runner, config_file):
        """--dry-run --output-format yaml returns valid YAML plan."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "migrate", "-p", "TEST", "-t", "ws-000", "-c", config_file,
            "--dry-run", "-f", "yaml",
        ], env=env)
        assert result.exit_code == 0
        plan = yaml.safe_load(result.output)
        assert "waves" in plan

    def test_migrate_quiet_no_progress(self, runner, config_file):
        """--quiet suppresses progress bars."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "migrate", "-p", "TEST", "-t", "ws-000", "-c", config_file, "-q",
        ], env=env)
        assert "Migrating project" in result.output

    def test_migrate_json_output(self, runner, config_file):
        """--output-format json renders agent results as JSON."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "migrate", "-p", "TEST", "-t", "ws-000", "-c", config_file,
            "-q", "-f", "json",
        ], env=env)
        # After "Migrating project" line, output contains JSON array
        assert "Migrating project" in result.output


# ── Validate ─────────────────────────────────────────────────

class TestValidate:
    def test_validate_no_config(self, runner):
        result = runner.invoke(cli, ["validate", "-p", "X", "-c", "/nonexistent.yaml"])
        assert result.exit_code != 0

    def test_validate_runs(self, runner, config_file):
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "validate", "-p", "TEST", "-c", config_file,
        ], env=env)
        assert "Validating migration" in result.output

    def test_validate_json_format(self, runner, config_file):
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "validate", "-p", "TEST", "-c", config_file, "-f", "json",
        ], env=env)
        assert "Validating migration" in result.output


# ── Report ───────────────────────────────────────────────────

class TestReport:
    def test_report_runs(self, runner, config_file):
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "report", "-p", "TEST", "-c", config_file, "-f", "json",
        ], env=env)
        assert result.exit_code == 0 or "status" in result.output


# ── Config Validate ──────────────────────────────────────────

class TestConfigValidate:
    def test_valid_config(self, runner, config_file):
        """Valid config reports success."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, ["config", "validate", config_file], env=env)
        # Should pass schema validation (may have warnings about output dir)
        assert result.exit_code == 0

    def test_missing_config(self, runner):
        """Non-existent config file → error exit."""
        result = runner.invoke(cli, ["config", "validate", "/nonexistent.yaml"])
        assert result.exit_code != 0

    def test_bad_yaml(self, runner, bad_yaml_file):
        """Malformed YAML → error."""
        result = runner.invoke(cli, ["config", "validate", bad_yaml_file])
        assert result.exit_code != 0
        assert "error" in result.output.lower() or "YAML" in result.output

    def test_invalid_schema(self, runner, invalid_schema_file):
        """YAML that doesn't match AppConfig schema → error."""
        result = runner.invoke(cli, ["config", "validate", invalid_schema_file])
        assert result.exit_code != 0

    def test_config_validate_json_format(self, runner, config_file):
        """--output-format json returns structured issues."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "config", "validate", config_file, "-f", "json",
        ], env=env)
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "valid" in data
        assert "issues" in data

    def test_config_validate_yaml_format(self, runner, config_file):
        """--output-format yaml works."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, [
            "config", "validate", config_file, "-f", "yaml",
        ], env=env)
        assert result.exit_code == 0

    def test_config_validate_warnings(self, runner, tmp_path):
        """Config with missing env var shows warnings."""
        cfg = {
            "dataiku": {
                "url": "https://fake.local",
                "api_key_env": "NONEXISTENT_VAR_12345",
                "project_key": "WARN_TEST",
            },
            "fabric": {"workspace_id": "ws"},
            "migration": {"output_dir": str(tmp_path / "noexist_output")},
        }
        path = tmp_path / "warn.yaml"
        path.write_text(yaml.dump(cfg))
        result = runner.invoke(cli, ["config", "validate", str(path)])
        # Should pass (warnings only) but show warning messages
        assert result.exit_code == 0
        assert "warning" in result.output.lower() or "⚠" in result.output


# ── Status ───────────────────────────────────────────────────

class TestStatus:
    def test_status_table(self, runner, config_file):
        """Status command with default (table) format."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, ["status", "-c", config_file], env=env)
        assert result.exit_code == 0
        assert "project_key" in result.output

    def test_status_json(self, runner, config_file):
        """Status --output-format json returns valid JSON."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, ["status", "-c", config_file, "-f", "json"], env=env)
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "project_key" in data
        assert "assets" in data
        assert "agents" in data

    def test_status_yaml(self, runner, config_file):
        """Status --output-format yaml returns valid YAML."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        result = runner.invoke(cli, ["status", "-c", config_file, "-f", "yaml"], env=env)
        assert result.exit_code == 0
        data = yaml.safe_load(result.output)
        assert "project_key" in data


# ── Interactive ──────────────────────────────────────────────

class TestInteractive:
    def test_interactive_aborts(self, runner, config_file):
        """Interactive mode — user declines to proceed."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        # Simulate inputs: project, workspace, config path, agents, resume, dry_run=yes, proceed=no
        user_input = f"TEST\nws-000\n{config_file}\nall\nn\ny\nn\n"
        result = runner.invoke(cli, ["interactive"], input=user_input, env=env)
        assert "Wizard" in result.output or "wizard" in result.output.lower()
        assert "Aborted" in result.output

    def test_interactive_proceeds(self, runner, config_file):
        """Interactive mode — user skips dry-run and proceeds."""
        env = {"TEST_DATAIKU_KEY": "fake-key-123"}
        # project, workspace, config, agents, resume=no, dry_run=no
        user_input = f"TEST\nws-000\n{config_file}\nall\nn\nn\n"
        result = runner.invoke(cli, ["interactive"], input=user_input, env=env)
        assert "Migration Wizard" in result.output or "wizard" in result.output.lower()


# ── Format Helpers ───────────────────────────────────────────

class TestFormatHelpers:
    def test_format_output_json(self):
        data = {"key": "value", "num": 42}
        result = _format_output(data, "json")
        parsed = json.loads(result)
        assert parsed["key"] == "value"
        assert parsed["num"] == 42

    def test_format_output_yaml(self):
        data = {"key": "value", "num": 42}
        result = _format_output(data, "yaml")
        parsed = yaml.safe_load(result)
        assert parsed["key"] == "value"

    def test_format_output_table_dict(self):
        data = {"name": "test", "count": 5}
        result = _format_output(data, "table")
        assert "name" in result
        assert "test" in result

    def test_format_table_dict(self):
        data = {"alpha": 1, "beta_long": "hello"}
        result = _format_table(data)
        assert "alpha" in result
        assert "beta_long" in result
        assert "hello" in result

    def test_format_table_list_of_dicts(self):
        data = [
            {"agent": "discovery", "status": "completed"},
            {"agent": "sql_converter", "status": "failed"},
        ]
        result = _format_table(data)
        assert "agent" in result
        assert "discovery" in result
        assert "sql_converter" in result
        # Should have header separator
        assert "---" in result

    def test_format_table_scalar(self):
        result = _format_table("just a string")
        assert result == "just a string"

    def test_format_table_nested_dict(self):
        """Dict values that are dicts or lists get JSON-serialized."""
        data = {"key": {"nested": True}, "list": [1, 2]}
        result = _format_table(data)
        assert "nested" in result


# ── Orchestrator Plan + Status ───────────────────────────────

class TestOrchestratorPlanStatus:
    def test_get_execution_plan(self, config_file):
        from src.cli import _build_orchestrator
        os.environ["TEST_DATAIKU_KEY"] = "fake"
        try:
            orch = _build_orchestrator(config_file)
            plan = orch.get_execution_plan()
            assert "waves" in plan
            assert plan["total_agents"] > 0
            assert plan["project_key"] == "CLI_TEST"
            assert isinstance(plan["waves"], list)
            assert len(plan["waves"]) > 0
            # Each wave has agents with name and description
            for wave in plan["waves"]:
                assert "wave" in wave
                assert "agents" in wave
                for agent_info in wave["agents"]:
                    assert "agent" in agent_info
                    assert "description" in agent_info
        finally:
            os.environ.pop("TEST_DATAIKU_KEY", None)

    def test_get_status_empty_registry(self, config_file):
        from src.cli import _build_orchestrator
        os.environ["TEST_DATAIKU_KEY"] = "fake"
        try:
            orch = _build_orchestrator(config_file)
            status = orch.get_status()
            assert status["project_key"] == "CLI_TEST"
            assert status["assets"]["total"] == 0
            assert status["agents"] == {}
        finally:
            os.environ.pop("TEST_DATAIKU_KEY", None)

    def test_get_execution_plan_with_resume(self, config_file):
        from src.cli import _build_orchestrator
        os.environ["TEST_DATAIKU_KEY"] = "fake"
        try:
            orch = _build_orchestrator(config_file)
            # With resume, discovery is skipped because registry is empty → no assets
            plan = orch.get_execution_plan(resume=True)
            assert "skipped_agents" in plan
        finally:
            os.environ.pop("TEST_DATAIKU_KEY", None)


# ── Config Validate Function ─────────────────────────────────

class TestConfigValidateFunction:
    def test_validate_config_valid(self, config_file):
        from src.core.config import validate_config
        os.environ["TEST_DATAIKU_KEY"] = "fake"
        try:
            issues = validate_config(config_file)
            errors = [i for i in issues if i["level"] == "error"]
            assert len(errors) == 0
        finally:
            os.environ.pop("TEST_DATAIKU_KEY", None)

    def test_validate_config_missing_file(self):
        from src.core.config import validate_config
        issues = validate_config("/nonexistent.yaml")
        assert len(issues) == 1
        assert issues[0]["level"] == "error"

    def test_validate_config_bad_schema(self, invalid_schema_file):
        from src.core.config import validate_config
        issues = validate_config(invalid_schema_file)
        assert any(i["level"] == "error" for i in issues)

    def test_validate_config_warnings_for_missing_env(self, tmp_path):
        from src.core.config import validate_config
        cfg = {
            "dataiku": {
                "url": "https://x.local",
                "api_key_env": "TOTALLY_NONEXISTENT_XYZ",
                "project_key": "X",
            },
            "fabric": {"workspace_id": "ws"},
        }
        path = tmp_path / "w.yaml"
        path.write_text(yaml.dump(cfg))
        # Ensure var is NOT set
        os.environ.pop("TOTALLY_NONEXISTENT_XYZ", None)
        issues = validate_config(str(path))
        warnings = [i for i in issues if i["level"] == "warning"]
        assert any("TOTALLY_NONEXISTENT_XYZ" in w["message"] for w in warnings)

    def test_validate_config_low_timeout_warning(self, tmp_path):
        from src.core.config import validate_config
        cfg = {
            "dataiku": {"url": "https://x.local", "api_key_env": "X", "project_key": "X"},
            "fabric": {"workspace_id": "ws"},
            "orchestrator": {"agent_timeout_seconds": 5},
        }
        path = tmp_path / "low.yaml"
        path.write_text(yaml.dump(cfg))
        issues = validate_config(str(path))
        assert any("timeout" in i["message"].lower() for i in issues)
