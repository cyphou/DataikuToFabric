"""Base agent interface — all migration agents implement this contract."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class AgentStatus(str, Enum):
    """Agent execution status."""

    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ABORTED = "aborted"


@dataclass
class AgentResult:
    """Result returned by an agent after execution."""

    agent_name: str
    status: AgentStatus
    assets_processed: int = 0
    assets_converted: int = 0
    assets_failed: int = 0
    review_flags: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Result of an agent's self-validation."""

    passed: bool
    checks_run: int = 0
    checks_passed: int = 0
    checks_failed: int = 0
    failures: list[str] = field(default_factory=list)


class BaseAgent(ABC):
    """Abstract base class for all migration agents."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique agent identifier."""

    @property
    @abstractmethod
    def description(self) -> str:
        """Human-readable description of what this agent does."""

    @abstractmethod
    async def execute(self, context: Any) -> AgentResult:
        """
        Run the agent's migration logic.

        Args:
            context: MigrationContext with config, registry, connectors, logger.

        Returns:
            AgentResult with status, counts, and diagnostics.
        """

    @abstractmethod
    async def validate(self, context: Any) -> ValidationResult:
        """Verify this agent's output is correct."""

    def rollback(self, context: Any) -> None:
        """Undo changes made by this agent (best-effort). Override if needed."""
