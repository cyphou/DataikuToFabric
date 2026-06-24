"""Integration tests for Wave B runtime observability wiring."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent, ValidationResult
from src.core.config import AppConfig
from src.core.orchestrator import Orchestrator
from src.core.registry import AssetRegistry


class DummyAgent(BaseAgent):
    @property
    def name(self) -> str:
        return "discovery"

    @property
    def description(self) -> str:
        return "dummy"

    async def execute(self, context):
        telemetry = context.connectors.get("decision_telemetry")
        if telemetry:
            telemetry.record(category="agent", decision="execute", agent=self.name, reason="dummy")
        return AgentResult(agent_name=self.name, status=AgentStatus.COMPLETED, assets_processed=1, assets_converted=1)

    async def validate(self, context):
        return ValidationResult(passed=True)


def _cfg(tmp_path: Path) -> AppConfig:
    return AppConfig.model_validate(
        {
            "dataiku": {"url": "https://example.local", "api_key_env": "TEST_KEY", "project_key": "PROJ"},
            "fabric": {"workspace_id": "ws1"},
            "migration": {"output_dir": str(tmp_path / "output"), "parallel_agents": False, "fail_fast": False},
            "orchestrator": {"max_retries": 1, "retry_delay_seconds": 0, "agent_timeout_seconds": 30},
            "logging": {"level": "WARNING", "format": "text"},
            "deployment": {"enable_snapshots": False, "enable_idempotency": False},
            "observability": {
                "enable_correlation_ids": True,
                "enable_decision_telemetry": True,
                "dashboard_dir": str(tmp_path / "output" / "ops"),
            },
        }
    )


def test_pipeline_writes_ops_dashboard(tmp_path):
    cfg = _cfg(tmp_path)
    registry = AssetRegistry(project_key="PROJ", registry_path=tmp_path / "output" / "registry.json")
    orch = Orchestrator(cfg, registry)
    orch.register_agent(DummyAgent())

    asyncio.run(orch.run_pipeline(agent_names=["discovery"]))

    dashboard_files = list((tmp_path / "output" / "ops").glob("ops_dashboard_*.json"))
    assert len(dashboard_files) == 1

    payload = json.loads(dashboard_files[0].read_text(encoding="utf-8"))
    assert payload["decision_telemetry"]["summary"]["events_count"] >= 2
    assert payload["agent_results"]["discovery"]["status"] == "completed"


def test_status_still_available_with_observability(tmp_path):
    cfg = _cfg(tmp_path)
    registry = AssetRegistry(project_key="PROJ", registry_path=tmp_path / "output" / "registry.json")
    orch = Orchestrator(cfg, registry)
    orch.register_agent(DummyAgent())
    asyncio.run(orch.run_pipeline(agent_names=["discovery"]))

    status = orch.get_status()
    assert "project_key" in status
    assert status["project_key"] == "PROJ"
