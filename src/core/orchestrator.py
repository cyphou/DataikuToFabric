"""Orchestration engine — coordinates agent execution with DAG-based dispatch."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent
from src.core.config import AppConfig
from src.core.logger import get_logger
from src.core.registry import AssetRegistry

logger = get_logger(__name__)


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
        """Run the full migration pipeline in dependency order."""
        if agent_names is None:
            agent_names = list(self._agents.keys())

        # Phase 1: Discovery (always first)
        if "discovery" in agent_names:
            await self.run_agent("discovery")
            if self.config.migration.fail_fast and self._results.get("discovery", AgentResult(
                agent_name="discovery", status=AgentStatus.FAILED
            )).status == AgentStatus.FAILED:
                return self._results

        # Phase 2: Connection mapping + Dataset migration (parallel)
        phase2 = [n for n in ["connection_mapper", "dataset_migrator"] if n in agent_names]
        if phase2:
            if self.config.migration.parallel_agents:
                await self.run_agents_parallel(phase2)
            else:
                for name in phase2:
                    await self.run_agent(name)

        # Phase 3: Recipe conversion (parallel)
        phase3 = [n for n in ["sql_converter", "python_converter", "visual_converter"] if n in agent_names]
        if phase3:
            if self.config.migration.parallel_agents:
                await self.run_agents_parallel(phase3)
            else:
                for name in phase3:
                    await self.run_agent(name)

        # Phase 4: Flow → Pipeline
        if "pipeline_builder" in agent_names:
            await self.run_agent("pipeline_builder")

        # Phase 5: Validation (always last)
        if "validator" in agent_names:
            await self.run_agent("validator")

        self.registry.save()
        return self._results

    def get_results(self) -> dict[str, AgentResult]:
        """Get all agent execution results."""
        return dict(self._results)
