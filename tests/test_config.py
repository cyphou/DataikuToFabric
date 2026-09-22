"""Tests for configuration loader and data models."""

from pathlib import Path

import pytest
import yaml

from src.core.config import AppConfig, DataikuConfig, FabricConfig, load_config


class TestAppConfig:
    def test_minimal_config(self, app_config):
        assert app_config.dataiku.project_key == "TEST_PROJECT"
        assert app_config.fabric.workspace_id == "00000000-0000-0000-0000-000000000000"
        assert app_config.migration.target_sql_dialect == "tsql"

    def test_defaults(self):
        cfg = AppConfig(
            dataiku=DataikuConfig(url="https://x.com", project_key="P"),
            fabric=FabricConfig(workspace_id="ws-1"),
        )
        assert cfg.orchestrator.max_retries == 3
        assert cfg.logging.level == "INFO"

    def test_proxy_url_defaults_to_none(self):
        cfg = DataikuConfig(url="https://x.com", project_key="P")
        assert cfg.proxy_url is None

    def test_proxy_url_can_be_set(self):
        cfg = DataikuConfig(url="https://x.com", project_key="P", proxy_url="http://proxy.corp:8080")
        assert cfg.proxy_url == "http://proxy.corp:8080"


class TestLoadConfig:
    def test_load_from_yaml(self, tmp_path):
        config_data = {
            "dataiku": {"url": "https://test.com", "project_key": "PROJ"},
            "fabric": {"workspace_id": "ws-123"},
        }
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(config_data))

        cfg = load_config(config_file)
        assert cfg.dataiku.project_key == "PROJ"

    def test_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            load_config("nonexistent.yaml")
