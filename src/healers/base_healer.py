"""Base healer — abstract class for all self-healing fixers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class HealerCategory(str, Enum):
    SQL = "sql"
    NOTEBOOK = "notebook"
    PIPELINE = "pipeline"
    SCHEMA = "schema"


@dataclass
class HealResult:
    """Result of a single heal operation."""
    healer_name: str
    asset_name: str
    applied: bool
    description: str
    before: str = ""
    after: str = ""


class BaseHealer(ABC):
    """Abstract base for self-healing fixers."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique healer name."""

    @property
    @abstractmethod
    def category(self) -> HealerCategory:
        """Category of issues this healer fixes."""

    @property
    @abstractmethod
    def description(self) -> str:
        """Short description of what this healer fixes."""

    @abstractmethod
    def can_heal(self, content: str, metadata: dict[str, Any]) -> bool:
        """Check if this healer can fix the given content."""

    @abstractmethod
    def heal(self, content: str, metadata: dict[str, Any]) -> HealResult:
        """Apply the fix and return the result."""


@dataclass
class HealerRegistry:
    """Registry of all available healers, grouped by category."""
    healers: list[BaseHealer] = field(default_factory=list)

    def register(self, healer: BaseHealer) -> None:
        self.healers.append(healer)

    def get_by_category(self, category: HealerCategory) -> list[BaseHealer]:
        return [h for h in self.healers if h.category == category]

    def heal_content(
        self,
        content: str,
        metadata: dict[str, Any],
        category: HealerCategory | None = None,
    ) -> tuple[str, list[HealResult]]:
        """Apply all applicable healers to content.

        Args:
            content: Source content (SQL, notebook JSON, etc.)
            metadata: Asset metadata for context.
            category: Optional filter to specific category.

        Returns:
            Tuple of (fixed_content, list_of_results).
        """
        results: list[HealResult] = []
        current = content
        applicable = self.healers if category is None else self.get_by_category(category)

        for healer in applicable:
            if healer.can_heal(current, metadata):
                result = healer.heal(current, metadata)
                if result.applied:
                    current = result.after or current
                    results.append(result)

        return current, results

    def heal_all(self, assets: list[Any]) -> list[str]:
        """Heal all assets. Returns list of fix descriptions."""
        fixes: list[str] = []
        for asset in assets:
            payload = asset.metadata.get("payload", "") if hasattr(asset, "metadata") else ""
            if not payload:
                continue

            _, results = self.heal_content(payload, getattr(asset, "metadata", {}))
            for r in results:
                fixes.append(f"{r.healer_name}: {r.description}")
                # Update the asset payload
                if r.after and hasattr(asset, "metadata"):
                    asset.metadata["payload"] = r.after

        return fixes
