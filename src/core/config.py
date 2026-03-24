"""Configuration loader — reads config.yaml and resolves environment variables."""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class DataikuConfig(BaseModel):
    url: str
    api_key_env: str = "DATAIKU_API_KEY"
    project_key: str
    timeout_seconds: int = 30
    max_retries: int = 3

    @property
    def api_key(self) -> str:
        value = os.environ.get(self.api_key_env, "")
        if not value:
            raise ValueError(f"Environment variable {self.api_key_env} is not set")
        return value


class FabricConfig(BaseModel):
    workspace_id: str
    lakehouse_name: str = "lh_migrated"
    warehouse_name: str = "wh_migrated"
    auth_method: str = "azure_cli"
    tenant_id_env: str = "AZURE_TENANT_ID"
    client_id_env: str = "AZURE_CLIENT_ID"


class MigrationConfig(BaseModel):
    target_sql_dialect: str = "tsql"
    default_storage: str = "lakehouse"
    parallel_agents: bool = True
    max_concurrent_agents: int = 4
    fail_fast: bool = False
    output_dir: str = "./output"


class OrchestratorConfig(BaseModel):
    max_retries: int = 3
    retry_delay_seconds: int = 5
    agent_timeout_seconds: int = 300
    circuit_breaker_threshold: int = 3
    report_format: list[str] = Field(default_factory=lambda: ["html", "json"])


class LoggingConfig(BaseModel):
    level: str = "INFO"
    format: str = "json"
    file: str = "./logs/migration.log"


class AppConfig(BaseModel):
    """Top-level application configuration."""

    dataiku: DataikuConfig
    fabric: FabricConfig
    migration: MigrationConfig = Field(default_factory=MigrationConfig)
    orchestrator: OrchestratorConfig = Field(default_factory=OrchestratorConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


def load_config(config_path: str | Path = "config/config.yaml") -> AppConfig:
    """Load and validate configuration from a YAML file."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    return AppConfig.model_validate(raw)


def validate_config(config_path: str | Path) -> list[dict[str, str]]:
    """Validate a config file and return a list of issues (empty = valid).

    Each issue is ``{"level": "error"|"warning", "message": "…"}``.
    """
    issues: list[dict[str, str]] = []
    path = Path(config_path)

    if not path.exists():
        issues.append({"level": "error", "message": f"Config file not found: {path}"})
        return issues

    try:
        with open(path) as f:
            raw = yaml.safe_load(f)
    except yaml.YAMLError as e:
        issues.append({"level": "error", "message": f"YAML parse error: {e}"})
        return issues

    if not isinstance(raw, dict):
        issues.append({"level": "error", "message": "Config root must be a mapping"})
        return issues

    # Schema validation via Pydantic
    try:
        cfg = AppConfig.model_validate(raw)
    except Exception as e:
        issues.append({"level": "error", "message": f"Schema validation failed: {e}"})
        return issues

    # Warnings for common problems
    if not os.environ.get(cfg.dataiku.api_key_env):
        issues.append({"level": "warning", "message": f"Env var {cfg.dataiku.api_key_env} is not set"})

    if cfg.orchestrator.agent_timeout_seconds < 30:
        issues.append({"level": "warning", "message": "Agent timeout < 30s may cause premature failures"})

    if cfg.migration.max_concurrent_agents > 8:
        issues.append({"level": "warning", "message": "max_concurrent_agents > 8 may overwhelm APIs"})

    output_dir = Path(cfg.migration.output_dir)
    if not output_dir.exists():
        issues.append({"level": "warning", "message": f"Output directory does not exist: {output_dir}"})

    return issues
