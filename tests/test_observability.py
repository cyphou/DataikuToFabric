"""Tests for Wave B observability helpers."""

from __future__ import annotations

import json
from pathlib import Path

from src.core.observability import (
    DecisionTelemetry,
    OpsDashboardWriter,
    get_correlation_id,
    new_correlation_id,
    set_correlation_id,
)


def test_correlation_id_roundtrip():
    cid = new_correlation_id("test")
    assert cid.startswith("test_")
    set_correlation_id(cid)
    assert get_correlation_id() == cid


def test_decision_telemetry_records_events():
    telemetry = DecisionTelemetry(correlation_id="corr_abc")
    telemetry.record(
        category="pipeline",
        decision="start",
        agent="discovery",
        reason="test run",
        metadata={"x": 1},
    )
    assert len(telemetry.events) == 1
    evt = telemetry.events[0]
    assert evt.category == "pipeline"
    assert evt.correlation_id == "corr_abc"


def test_decision_telemetry_summary():
    telemetry = DecisionTelemetry(correlation_id="corr_xyz")
    telemetry.record(category="wave", decision="start_wave", agent="a1")
    telemetry.record(category="wave", decision="start_wave", agent="a1")
    telemetry.record(category="resume", decision="skip_agent", agent="a2")
    summary = telemetry.summary()
    assert summary["events_count"] == 3
    assert summary["by_category"]["wave"] == 2
    assert summary["by_agent"]["a1"] == 2


def test_ops_dashboard_writer(tmp_path):
    telemetry = DecisionTelemetry(correlation_id="corr_ops")
    telemetry.record(category="pipeline", decision="complete")

    writer = OpsDashboardWriter(tmp_path)
    path = writer.write(
        correlation_id="corr_ops",
        project_key="PROJ",
        agent_results={"discovery": {"status": "completed"}},
        asset_stats={"total": 1},
        decision_telemetry=telemetry,
    )

    assert path.exists()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["correlation_id"] == "corr_ops"
    assert payload["project_key"] == "PROJ"
    assert payload["decision_telemetry"]["summary"]["events_count"] == 1
