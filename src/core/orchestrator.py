"""Orchestration engine — coordinates agent execution with DAG-based dispatch."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import networkx as nx

from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent
from src.core.config import AppConfig
from src.core.deployment import DeploymentManifestStore, IdempotencyChecker
from src.core.logger import get_logger
from src.core.observability import (
    DecisionTelemetry,
    OpsDashboardWriter,
    get_correlation_id,
    new_correlation_id,
    set_correlation_id,
)
from src.core.registry import AssetRegistry
from src.core.snapshot import SnapshotStore, capture_snapshot

logger = get_logger(__name__)

# ── Default agent execution order ─────────────────────────────
# Each tuple is (agent_name, [dependencies])
DEFAULT_AGENT_GRAPH: list[tuple[str, list[str]]] = [
    ("discovery", []),
    ("connection_mapper", ["discovery"]),
    ("dataset_migrator", ["discovery"]),
    ("sql_converter", ["discovery"]),
    ("python_converter", ["discovery"]),
    ("visual_converter", ["discovery"]),
    ("pipeline_builder", ["sql_converter", "python_converter", "visual_converter"]),
    ("validator", ["dataset_migrator", "connection_mapper", "pipeline_builder"]),
]


def build_agent_dag(
    agent_names: list[str],
    graph_spec: list[tuple[str, list[str]]] | None = None,
) -> nx.DiGraph:
    """Build a DAG of agent execution dependencies.

    Only includes agents that are in ``agent_names``. Dependencies
    on missing agents are silently dropped.
    """
    spec = graph_spec or DEFAULT_AGENT_GRAPH
    available = set(agent_names)
    dag = nx.DiGraph()

    for name, deps in spec:
        if name not in available:
            continue
        dag.add_node(name)
        for dep in deps:
            if dep in available:
                dag.add_edge(dep, name)

    return dag


def get_execution_waves(dag: nx.DiGraph) -> list[list[str]]:
    """Return agents grouped into parallel execution waves.

    Each wave contains agents whose dependencies have all been
    satisfied by previous waves.
    """
    waves: list[list[str]] = []
    remaining = set(dag.nodes())

    while remaining:
        # Find nodes whose predecessors are all done
        ready = []
        for node in remaining:
            preds = set(dag.predecessors(node))
            if preds.issubset(set().union(*(waves or [[]]))):
                # All predecessors in previous waves
                ready.append(node)

        # More robust: check all predecessors are NOT in remaining
        ready = [
            n for n in remaining
            if all(p not in remaining for p in dag.predecessors(n))
        ]

        if not ready:
            # Shouldn't happen with a DAG, but guard against cycles
            logger.warning("agent_dag_deadlock", remaining=list(remaining))
            waves.append(sorted(remaining))
            break

        waves.append(sorted(ready))
        remaining -= set(ready)

    return waves


@dataclass
class MigrationContext:
    """Shared context passed to every agent."""

    config: AppConfig
    registry: AssetRegistry
    connectors: dict[str, Any] = field(default_factory=dict)


class Orchestrator:
    """DAG-based agent orchestrator with parallel dispatch and retry logic."""

    def __init__(self, config: AppConfig, registry: AssetRegistry):
        self.config = config
        self.registry = registry
        self.context = MigrationContext(config=config, registry=registry)
        self._agents: dict[str, BaseAgent] = {}
        self._results: dict[str, AgentResult] = {}
        self._consecutive_failures: int = 0
        self._checkpoints: list[Path] = []

    def _new_run_id(self) -> str:
        """Generate a stable run ID for manifests and snapshots."""
        return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    @staticmethod
    def _asset_content_payload(asset: Any) -> dict[str, Any]:
        """Build deterministic asset payload used for idempotency hashing."""
        try:
            data = asset.model_dump(mode="json")
        except Exception:
            data = dict(vars(asset))

        # Volatile timestamp fields must be ignored to avoid false-positive changes.
        data.pop("timestamps", None)
        return data

    @staticmethod
    def _fabric_item_id(asset: Any) -> str | None:
        """Extract common Fabric item identifiers from target metadata."""
        target = getattr(asset, "target_fabric_asset", None) or {}
        if not isinstance(target, dict):
            return None
        return target.get("id") or target.get("item_id") or target.get("fabric_item_id")

    def _snapshot_store(self) -> SnapshotStore:
        """Return the configured snapshot store."""
        dep_cfg = getattr(self.config, "deployment", None)
        snapshot_dir = Path(
            getattr(dep_cfg, "snapshot_dir", Path(self.config.migration.output_dir) / "snapshots")
        )
        return SnapshotStore(snapshot_dir)

    def _manifest_store(self) -> DeploymentManifestStore:
        """Return the configured deployment manifest store."""
        dep_cfg = getattr(self.config, "deployment", None)
        manifest_dir = Path(
            getattr(dep_cfg, "manifest_dir", Path(self.config.migration.output_dir) / "manifests")
        )
        return DeploymentManifestStore(manifest_dir)

    def register_agent(self, agent: BaseAgent) -> None:
        """Register an agent for orchestration."""
        self._agents[agent.name] = agent

    async def run_agent(self, agent_name: str) -> AgentResult:
        """Run a single agent with retry logic, timeout, and circuit breaker."""
        # Circuit breaker: skip if too many consecutive failures
        cb_threshold = self.config.orchestrator.circuit_breaker_threshold
        if self._consecutive_failures >= cb_threshold:
            logger.warning("circuit_breaker_open", agent=agent_name,
                           consecutive_failures=self._consecutive_failures)
            result = AgentResult(
                agent_name=agent_name,
                status=AgentStatus.ABORTED,
                errors=[f"Circuit breaker open after {self._consecutive_failures} consecutive failures"],
            )
            self._results[agent_name] = result
            return result

        agent = self._agents[agent_name]
        max_retries = self.config.orchestrator.max_retries
        delay = self.config.orchestrator.retry_delay_seconds
        timeout = self.config.orchestrator.agent_timeout_seconds

        for attempt in range(1, max_retries + 1):
            try:
                logger.info("agent_start", agent=agent_name, attempt=attempt)
                result = await asyncio.wait_for(
                    agent.execute(self.context),
                    timeout=timeout if timeout > 0 else None,
                )
                self._results[agent_name] = result

                if result.status == AgentStatus.COMPLETED:
                    logger.info("agent_completed", agent=agent_name,
                                processed=result.assets_processed,
                                converted=result.assets_converted)
                    self._consecutive_failures = 0  # reset circuit breaker
                    return result

                if result.status == AgentStatus.FAILED and attempt < max_retries:
                    logger.warning("agent_retry", agent=agent_name, attempt=attempt,
                                   errors=result.errors)
                    await asyncio.sleep(delay * attempt)  # exponential backoff
                    continue

                self._consecutive_failures += 1
                return result

            except asyncio.TimeoutError:
                logger.error("agent_timeout", agent=agent_name, timeout=timeout,
                             attempt=attempt)
                if attempt < max_retries:
                    await asyncio.sleep(delay * attempt)
                    continue
                self._consecutive_failures += 1
                result = AgentResult(
                    agent_name=agent_name,
                    status=AgentStatus.FAILED,
                    errors=[f"Agent timed out after {timeout}s"],
                )
                self._results[agent_name] = result
                return result

            except Exception as e:
                logger.error("agent_exception", agent=agent_name, error=str(e))
                if attempt < max_retries:
                    await asyncio.sleep(delay * attempt)
                    continue
                self._consecutive_failures += 1
                result = AgentResult(
                    agent_name=agent_name,
                    status=AgentStatus.FAILED,
                    errors=[str(e)],
                )
                self._results[agent_name] = result
                return result

        self._consecutive_failures += 1
        result = AgentResult(
            agent_name=agent_name,
            status=AgentStatus.ABORTED,
            errors=["Max retries exceeded"],
        )
        self._results[agent_name] = result
        return result

    async def run_agents_parallel(self, agent_names: list[str]) -> list[AgentResult]:
        """Run multiple agents concurrently."""
        max_concurrent = self.config.migration.max_concurrent_agents
        semaphore = asyncio.Semaphore(max_concurrent)

        async def _run_with_semaphore(name: str) -> AgentResult:
            async with semaphore:
                return await self.run_agent(name)

        results = await asyncio.gather(
            *[_run_with_semaphore(name) for name in agent_names],
            return_exceptions=True,
        )

        agent_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                agent_results.append(AgentResult(
                    agent_name=agent_names[i],
                    status=AgentStatus.FAILED,
                    errors=[str(result)],
                ))
            else:
                agent_results.append(result)

        return agent_results

    async def run_pipeline(
        self,
        agent_names: list[str] | None = None,
        *,
        resume: bool = False,
        rerun_agents: list[str] | None = None,
        asset_ids: set[str] | None = None,
        checkpoint_dir: Path | None = None,
        keep_checkpoints: bool = False,
        on_agent_start: Any | None = None,
        on_agent_done: Any | None = None,
    ) -> dict[str, AgentResult]:
        """Run the full migration pipeline using DAG-based execution waves.

        Args:
            agent_names: Specific agents to run (default: all registered).
            resume: If True, skip agents whose assets are already completed.
            rerun_agents: Agent names to force-rerun (resets their assets first).
            asset_ids: If set, only process these specific asset IDs.
            checkpoint_dir: Directory for checkpoint files (default: output/).
            keep_checkpoints: Keep checkpoint files after successful completion.
        """
        if agent_names is None:
            agent_names = list(self._agents.keys())

        obs_cfg = getattr(self.config, "observability", None)
        enable_corr = getattr(obs_cfg, "enable_correlation_ids", True)
        enable_telemetry = getattr(obs_cfg, "enable_decision_telemetry", True)
        dashboard_dir = Path(
            getattr(obs_cfg, "dashboard_dir", Path(self.config.migration.output_dir) / "ops")
        )

        correlation_id = get_correlation_id()
        if enable_corr and not correlation_id:
            correlation_id = new_correlation_id("run")
            set_correlation_id(correlation_id)

        decision_telemetry = DecisionTelemetry(correlation_id=correlation_id)
        self.context.connectors["decision_telemetry"] = decision_telemetry
        self.context.connectors["correlation_id"] = correlation_id

        if enable_telemetry:
            decision_telemetry.record(
                category="pipeline",
                decision="start",
                reason="pipeline execution requested",
                metadata={"agent_names": agent_names},
            )

        run_id = self._new_run_id()

        dep_cfg = getattr(self.config, "deployment", None)
        enable_snapshots = getattr(dep_cfg, "enable_snapshots", True)
        enable_idempotency = getattr(dep_cfg, "enable_idempotency", True)

        # Capture pre-deploy snapshot for rollback reliability.
        if enable_snapshots:
            snapshot = capture_snapshot(run_id, self.registry.project_key, self.registry)
            self._snapshot_store().save(snapshot)

        # Build idempotency checker for deployment manifest traceability.
        idempotency_checker: IdempotencyChecker | None = None
        if enable_idempotency:
            idempotency_checker = IdempotencyChecker(
                store=self._manifest_store(),
                run_id=run_id,
                project_key=self.registry.project_key,
            )
            idempotency_checker.load_previous()

        def _finalize_operational_artifacts() -> None:
            if idempotency_checker is None:
                return

            for asset in self.registry.get_all():
                payload = self._asset_content_payload(asset)
                asset_type = asset.type.value if hasattr(asset.type, "value") else str(asset.type)

                if idempotency_checker.needs_deploy(asset.id, payload):
                    idempotency_checker.record_deployed(
                        asset_id=asset.id,
                        asset_type=asset_type,
                        asset_name=asset.name,
                        content=payload,
                        fabric_item_id=self._fabric_item_id(asset),
                    )
                else:
                    idempotency_checker.record_skipped(
                        asset_id=asset.id,
                        asset_type=asset_type,
                        asset_name=asset.name,
                        content=payload,
                    )

            manifest_path = idempotency_checker.finalize()
            logger.info("deployment_manifest_finalized", run_id=run_id, path=str(manifest_path))

        # ── Resume: load checkpoint and detect completed agents ──
        skip_agents: set[str] = set()
        if resume:
            completed = self.registry.get_completed_agents()
            skip_agents = completed & set(agent_names)
            if skip_agents:
                logger.info("resume_skipping", agents=sorted(skip_agents))
                if enable_telemetry:
                    for name in sorted(skip_agents):
                        decision_telemetry.record(
                            category="resume",
                            decision="skip_agent",
                            agent=name,
                            reason="already completed from checkpoint",
                        )

        # ── Selective re-run: reset assets for specified agents ──
        if rerun_agents:
            for agent_name in rerun_agents:
                count = self.registry.reset_assets_for_agent(agent_name)
                logger.info("rerun_reset", agent=agent_name, assets_reset=count)
                if enable_telemetry:
                    decision_telemetry.record(
                        category="rerun",
                        decision="reset_agent_assets",
                        agent=agent_name,
                        reason="forced rerun requested",
                        metadata={"assets_reset": count},
                    )
                skip_agents.discard(agent_name)
                # Also force-run downstream agents
                dag = build_agent_dag(agent_names)
                if agent_name in dag:
                    for downstream in nx.descendants(dag, agent_name):
                        self.registry.reset_assets_for_agent(downstream)
                        skip_agents.discard(downstream)
                        if enable_telemetry:
                            decision_telemetry.record(
                                category="rerun",
                                decision="reset_downstream_assets",
                                agent=downstream,
                                reason=f"upstream agent {agent_name} was rerun",
                            )

        # ── Asset-level filter ──
        if asset_ids:
            self.registry.filter_asset_ids(asset_ids)
            # Discovery is not needed if we already have filtered assets
            skip_agents.add("discovery")
            if enable_telemetry:
                decision_telemetry.record(
                    category="asset_filter",
                    decision="filter_asset_ids",
                    reason="asset_ids provided",
                    metadata={"asset_ids_count": len(asset_ids)},
                )

        # Build the execution DAG and compute waves
        active_agents = [n for n in agent_names if n not in skip_agents]
        dag = build_agent_dag(active_agents)
        waves = get_execution_waves(dag)

        # Pre-fill results for skipped agents
        for name in skip_agents:
            self._results[name] = AgentResult(
                agent_name=name,
                status=AgentStatus.COMPLETED,
                details={"skipped": True, "reason": "resumed_from_checkpoint"},
            )

        ckpt_dir = checkpoint_dir or Path(self.config.migration.output_dir)
        logger.info("pipeline_start", waves=[[n for n in w] for w in waves],
                     total_agents=len(active_agents),
                     skipped_agents=sorted(skip_agents))

        for wave_idx, wave in enumerate(waves):
            logger.info("wave_start", wave=wave_idx + 1, agents=wave)
            if enable_telemetry:
                decision_telemetry.record(
                    category="wave",
                    decision="start_wave",
                    reason=f"wave {wave_idx + 1} starting",
                    metadata={"wave": wave_idx + 1, "agents": wave},
                )

            if len(wave) > 1 and self.config.migration.parallel_agents:
                for name in wave:
                    if on_agent_start:
                        on_agent_start(name, wave_idx + 1)
                await self.run_agents_parallel(wave)
                for name in wave:
                    if on_agent_done:
                        on_agent_done(name, self._results.get(name))
            else:
                for name in wave:
                    if on_agent_start:
                        on_agent_start(name, wave_idx + 1)
                    await self.run_agent(name)
                    if on_agent_done:
                        on_agent_done(name, self._results.get(name))

            # Save checkpoint after each wave
            ckpt = self.registry.save_checkpoint(ckpt_dir, wave_idx + 1)
            self._checkpoints.append(ckpt)

            # Check fail-fast after each wave
            if self.config.migration.fail_fast:
                for name in wave:
                    result = self._results.get(name)
                    if result and result.status == AgentStatus.FAILED:
                        logger.warning("fail_fast_abort", failed_agent=name, wave=wave_idx + 1)
                        if enable_telemetry:
                            decision_telemetry.record(
                                category="fail_fast",
                                decision="abort_pipeline",
                                agent=name,
                                reason="agent failure while fail_fast enabled",
                                metadata={"wave": wave_idx + 1},
                            )
                        self.registry.save()
                        _finalize_operational_artifacts()
                        OpsDashboardWriter(dashboard_dir).write(
                            correlation_id=correlation_id or "unknown",
                            project_key=self.registry.project_key,
                            agent_results={
                                n: {
                                    "status": r.status.value,
                                    "processed": r.assets_processed,
                                    "converted": r.assets_converted,
                                    "failed": r.assets_failed,
                                }
                                for n, r in self._results.items()
                            },
                            asset_stats=self.registry.get_statistics(),
                            decision_telemetry=decision_telemetry,
                            manifests={"last_manifest": self.get_status().get("last_manifest")},
                            snapshots={"last_snapshot": self.get_status().get("last_snapshot")},
                        )
                        return self._results

        self.registry.save()
        _finalize_operational_artifacts()

        if enable_telemetry:
            decision_telemetry.record(
                category="pipeline",
                decision="complete",
                reason="pipeline execution finished",
                metadata={"agents": len(active_agents), "skipped_agents": len(skip_agents)},
            )

        OpsDashboardWriter(dashboard_dir).write(
            correlation_id=correlation_id or "unknown",
            project_key=self.registry.project_key,
            agent_results={
                n: {
                    "status": r.status.value,
                    "processed": r.assets_processed,
                    "converted": r.assets_converted,
                    "failed": r.assets_failed,
                }
                for n, r in self._results.items()
            },
            asset_stats=self.registry.get_statistics(),
            decision_telemetry=decision_telemetry,
            manifests={"last_manifest": self.get_status().get("last_manifest")},
            snapshots={"last_snapshot": self.get_status().get("last_snapshot")},
        )

        # Cleanup intermediate checkpoints on success
        if not keep_checkpoints:
            self._cleanup_checkpoints()

        return self._results

    def _cleanup_checkpoints(self) -> None:
        """Remove intermediate checkpoint files."""
        for ckpt in self._checkpoints:
            try:
                if ckpt.exists():
                    ckpt.unlink()
            except OSError:
                pass
        self._checkpoints.clear()

    def get_results(self) -> dict[str, AgentResult]:
        """Get all agent execution results."""
        return dict(self._results)

    def get_execution_plan(
        self,
        agent_names: list[str] | None = None,
        *,
        resume: bool = False,
    ) -> dict[str, Any]:
        """Return the execution plan without running anything (dry-run).

        Returns a dict with waves, agent info, and asset counts.
        """
        if agent_names is None:
            agent_names = list(self._agents.keys())

        skip_agents: set[str] = set()
        if resume:
            completed = self.registry.get_completed_agents()
            skip_agents = completed & set(agent_names)

        active_agents = [n for n in agent_names if n not in skip_agents]
        dag = build_agent_dag(active_agents)
        waves = get_execution_waves(dag)

        # Gather asset counts per agent type
        from src.models.asset import AssetType
        agent_type_map: dict[str, list[str]] = {
            "discovery": [],
            "sql_converter": [AssetType.RECIPE_SQL.value],
            "python_converter": [AssetType.RECIPE_PYTHON.value],
            "visual_converter": [AssetType.RECIPE_VISUAL.value],
            "dataset_migrator": [AssetType.DATASET.value],
            "connection_mapper": [AssetType.CONNECTION.value],
            "pipeline_builder": [AssetType.FLOW.value, AssetType.SCENARIO.value],
            "validator": [],
        }

        wave_plans = []
        for wave_idx, wave in enumerate(waves):
            wave_info = []
            for agent_name in wave:
                agent = self._agents.get(agent_name)
                type_values = agent_type_map.get(agent_name, [])
                asset_count = len([
                    a for a in self.registry.get_all()
                    if a.type.value in type_values
                ]) if type_values else len(self.registry.get_all())
                wave_info.append({
                    "agent": agent_name,
                    "description": agent.description if agent else "",
                    "asset_count": asset_count,
                })
            wave_plans.append({"wave": wave_idx + 1, "agents": wave_info})

        return {
            "project_key": self.registry.project_key,
            "total_agents": len(active_agents),
            "skipped_agents": sorted(skip_agents),
            "waves": wave_plans,
            "parallel": self.config.migration.parallel_agents,
            "fail_fast": self.config.migration.fail_fast,
            "max_retries": self.config.orchestrator.max_retries,
            "timeout_seconds": self.config.orchestrator.agent_timeout_seconds,
        }

    def get_status(self) -> dict[str, Any]:
        """Return the current migration status from the registry."""
        stats = self.registry.get_statistics()
        agent_status = {}
        for name, result in self._results.items():
            agent_status[name] = {
                "status": result.status.value,
                "processed": result.assets_processed,
                "converted": result.assets_converted,
                "failed": result.assets_failed,
                "errors": result.errors,
                "review_flags": result.review_flags,
            }

        # Check for last checkpoint
        ckpt_dir = Path(self.config.migration.output_dir)
        checkpoints = sorted(ckpt_dir.glob("checkpoint_wave_*.json")) if ckpt_dir.exists() else []
        last_checkpoint = str(checkpoints[-1]) if checkpoints else None

        dep_cfg = getattr(self.config, "deployment", None)
        manifest_dir = Path(
            getattr(dep_cfg, "manifest_dir", Path(self.config.migration.output_dir) / "manifests")
        )
        manifests = sorted(manifest_dir.glob("deployment_*.json")) if manifest_dir.exists() else []
        last_manifest = str(manifests[-1]) if manifests else None

        snapshot_dir = Path(
            getattr(dep_cfg, "snapshot_dir", Path(self.config.migration.output_dir) / "snapshots")
        )
        snapshots = sorted(snapshot_dir.glob("snapshot_*.json")) if snapshot_dir.exists() else []
        last_snapshot = str(snapshots[-1]) if snapshots else None

        return {
            "project_key": self.registry.project_key,
            "registry_path": str(self.registry.registry_path),
            "last_checkpoint": last_checkpoint,
            "last_manifest": last_manifest,
            "last_snapshot": last_snapshot,
            "assets": stats,
            "agents": agent_status,
        }
