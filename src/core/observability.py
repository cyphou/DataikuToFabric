"""Wave B observability helpers: correlation IDs and decision telemetry.

This module centralizes lightweight observability primitives that can be used by:
- Orchestrator pipeline runs
- Agent conversion decisions
- API request lifecycle

Design goals:
- No heavy dependencies
- Backward-compatible defaults
- Deterministic JSON outputs for downstream ingestion
"""

from __future__ import annotations

import contextvars
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.core.logger import get_logger

logger = get_logger(__name__)


# ── Correlation ID context ────────────────────────────────────────────────────

_CORRELATION_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_id", default=None
)


def new_correlation_id(prefix: str = "corr") -> str:
    """Return a new correlation ID with a readable prefix."""
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def set_correlation_id(correlation_id: str) -> None:
    """Set the current context correlation ID."""
    _CORRELATION_ID.set(correlation_id)


def get_correlation_id(default: str | None = None) -> str | None:
    """Get current context correlation ID, or *default* if not set."""
    return _CORRELATION_ID.get() or default


# ── Decision telemetry models ─────────────────────────────────────────────────


class DecisionEvent:
    """Single decision point recorded during migration execution."""

    def __init__(
        self,
        *,
        category: str,
        decision: str,
        agent: str | None = None,
        asset_id: str | None = None,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        timestamp: str | None = None,
    ) -> None:
        self.category = category
        self.decision = decision
        self.agent = agent
        self.asset_id = asset_id
        self.reason = reason
        self.metadata = metadata or {}
        self.correlation_id = correlation_id or get_correlation_id()
        self.timestamp = timestamp or datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "correlation_id": self.correlation_id,
            "category": self.category,
            "agent": self.agent,
            "asset_id": self.asset_id,
            "decision": self.decision,
            "reason": self.reason,
            "metadata": self.metadata,
        }


class DecisionTelemetry:
    """In-memory decision telemetry collector for a pipeline run."""

    def __init__(self, correlation_id: str | None = None) -> None:
        self.correlation_id = correlation_id or get_correlation_id() or new_correlation_id()
        self._events: list[DecisionEvent] = []

    def record(
        self,
        *,
        category: str,
        decision: str,
        agent: str | None = None,
        asset_id: str | None = None,
        reason: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        event = DecisionEvent(
            category=category,
            decision=decision,
            agent=agent,
            asset_id=asset_id,
            reason=reason,
            metadata=metadata,
            correlation_id=self.correlation_id,
        )
        self._events.append(event)
        logger.info(
            "decision_telemetry",
            correlation_id=self.correlation_id,
            category=category,
            agent=agent,
            asset_id=asset_id,
            decision=decision,
            reason=reason,
        )

    @property
    def events(self) -> list[DecisionEvent]:
        return list(self._events)

    def summary(self) -> dict[str, Any]:
        by_category: dict[str, int] = {}
        by_agent: dict[str, int] = {}

        for e in self._events:
            by_category[e.category] = by_category.get(e.category, 0) + 1
            if e.agent:
                by_agent[e.agent] = by_agent.get(e.agent, 0) + 1

        return {
            "correlation_id": self.correlation_id,
            "events_count": len(self._events),
            "by_category": by_category,
            "by_agent": by_agent,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary(),
            "events": [e.to_dict() for e in self._events],
        }


class OpsDashboardWriter:
    """Writes a run-level operations dashboard JSON artifact."""

    def __init__(self, output_dir: Path) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write(
        self,
        *,
        correlation_id: str,
        project_key: str,
        agent_results: dict[str, Any],
        asset_stats: dict[str, Any],
        decision_telemetry: DecisionTelemetry,
        manifests: dict[str, Any] | None = None,
        snapshots: dict[str, Any] | None = None,
    ) -> Path:
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "correlation_id": correlation_id,
            "project_key": project_key,
            "agent_results": agent_results,
            "asset_stats": asset_stats,
            "decision_telemetry": decision_telemetry.to_dict(),
            "manifests": manifests or {},
            "snapshots": snapshots or {},
        }

        path = self.output_dir / f"ops_dashboard_{correlation_id}.json"
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        logger.info("ops_dashboard_written", correlation_id=correlation_id, path=str(path))
        return path
