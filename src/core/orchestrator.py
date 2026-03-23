"""Orchestration engine — coordinates agent execution with DAG-based dispatch."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
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

    def register_agent(self, agent: BaseAgent) -> None:
        """Register an agent for orchestration."""
        self._agents[agent.name] = agent

    async def run_agent(self, agent_name: str) -> AgentResult:
        """Run a single agent with retry logic."""
        agent = self._agents[agent_name]
        max_retries = self.config.orchestrator.max_retries
        delay = self.config.orchestrator.retry_delay_seconds

        for attempt in range(1, max_retries + 1):
            try:
                logger.info("agent_start", agent=agent_name, attempt=attempt)
                result = await agent.execute(self.context)
                self._results[agent_name] = result

                if result.status == AgentStatus.COMPLETED:
                    logger.info("agent_completed", agent=agent_name,
                                processed=result.assets_processed,
                                converted=result.assets_converted)
                    return result

                if result.status == AgentStatus.FAILED and attempt < max_retries:
                    logger.warning("agent_retry", agent=agent_name, attempt=attempt,
                                   errors=result.errors)
                    await asyncio.sleep(delay * attempt)  # exponential backoff
                    continue

                return result

            except Exception as e:
                logger.error("agent_exception", agent=agent_name, error=str(e))
                if attempt < max_retries:
                    await asyncio.sleep(delay * attempt)
                    continue
                return AgentResult(
                    agent_name=agent_name,
                    status=AgentStatus.FAILED,
                    errors=[str(e)],
                )

        return AgentResult(
            agent_name=agent_name,
            status=AgentStatus.ABORTED,
            errors=["Max retries exceeded"],
        )

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

    async def run_pipeline(self, agent_names: list[str] | None = None) -> dict[str, AgentResult]:
        """Run the full migration pipeline using DAG-based execution waves.

        Agents are executed in topological order based on their dependency
        graph. Independent agents within the same wave run in parallel
        (if ``parallel_agents`` is enabled in config).
        """
        if agent_names is None:
            agent_names = list(self._agents.keys())

        # Build the execution DAG and compute waves
        dag = build_agent_dag(agent_names)
        waves = get_execution_waves(dag)

        logger.info("pipeline_start", waves=[[n for n in w] for w in waves],
                     total_agents=len(agent_names))

        for wave_idx, wave in enumerate(waves):
            logger.info("wave_start", wave=wave_idx + 1, agents=wave)

            if len(wave) > 1 and self.config.migration.parallel_agents:
                await self.run_agents_parallel(wave)
            else:
                for name in wave:
                    await self.run_agent(name)

            # Check fail-fast after each wave
            if self.config.migration.fail_fast:
                for name in wave:
                    result = self._results.get(name)
                    if result and result.status == AgentStatus.FAILED:
                        logger.warning("fail_fast_abort", failed_agent=name, wave=wave_idx + 1)
                        self.registry.save()
                        return self._results

        self.registry.save()
        return self._results

    def get_results(self) -> dict[str, AgentResult]:
        """Get all agent execution results."""
        return dict(self._results)
