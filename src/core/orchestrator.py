"""Orchestration engine — coordinates agent execution with DAG-based dispatch."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import networkx as nx

from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent
from src.core.config import AppConfig
from src.core.logger import get_logger
from src.core.registry import AssetRegistry

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

        # ── Resume: load checkpoint and detect completed agents ──
        skip_agents: set[str] = set()
        if resume:
            completed = self.registry.get_completed_agents()
            skip_agents = completed & set(agent_names)
            if skip_agents:
                logger.info("resume_skipping", agents=sorted(skip_agents))

        # ── Selective re-run: reset assets for specified agents ──
        if rerun_agents:
            for agent_name in rerun_agents:
                count = self.registry.reset_assets_for_agent(agent_name)
                logger.info("rerun_reset", agent=agent_name, assets_reset=count)
                skip_agents.discard(agent_name)
                # Also force-run downstream agents
                dag = build_agent_dag(agent_names)
                if agent_name in dag:
                    for downstream in nx.descendants(dag, agent_name):
                        self.registry.reset_assets_for_agent(downstream)
                        skip_agents.discard(downstream)

        # ── Asset-level filter ──
        if asset_ids:
            self.registry.filter_asset_ids(asset_ids)
            # Discovery is not needed if we already have filtered assets
            skip_agents.add("discovery")

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

            if len(wave) > 1 and self.config.migration.parallel_agents:
                await self.run_agents_parallel(wave)
            else:
                for name in wave:
                    await self.run_agent(name)

            # Save checkpoint after each wave
            ckpt = self.registry.save_checkpoint(ckpt_dir, wave_idx + 1)
            self._checkpoints.append(ckpt)

            # Check fail-fast after each wave
            if self.config.migration.fail_fast:
                for name in wave:
                    result = self._results.get(name)
                    if result and result.status == AgentStatus.FAILED:
                        logger.warning("fail_fast_abort", failed_agent=name, wave=wave_idx + 1)
                        self.registry.save()
                        return self._results

        self.registry.save()

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
