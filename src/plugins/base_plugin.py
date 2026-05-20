"""Base plugin — abstract class defining the plugin hook interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BasePlugin(ABC):
    """Abstract base for migration plugins.

    Plugins can hook into 7 lifecycle points:
    - on_discovery_complete: After asset discovery
    - on_before_convert: Before each asset conversion
    - on_after_convert: After each asset conversion
    - on_before_validate: Before validation stage
    - on_after_validate: After validation stage
    - on_before_deploy: Before deployment stage
    - on_after_deploy: After deployment stage
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique plugin name."""

    @property
    @abstractmethod
    def version(self) -> str:
        """Plugin version string."""

    @property
    def description(self) -> str:
        return ""

    def on_discovery_complete(self, assets: list[Any], context: dict[str, Any]) -> None:
        """Called after asset discovery completes."""

    def on_before_convert(self, asset: Any, context: dict[str, Any]) -> Any:
        """Called before converting an asset. Return modified asset or None."""
        return asset

    def on_after_convert(self, asset: Any, result: Any, context: dict[str, Any]) -> None:
        """Called after converting an asset."""

    def on_before_validate(self, assets: list[Any], context: dict[str, Any]) -> None:
        """Called before validation stage."""

    def on_after_validate(self, assets: list[Any], results: Any, context: dict[str, Any]) -> None:
        """Called after validation completes."""

    def on_before_deploy(self, assets: list[Any], context: dict[str, Any]) -> None:
        """Called before deployment stage."""

    def on_after_deploy(self, assets: list[Any], results: Any, context: dict[str, Any]) -> None:
        """Called after deployment completes."""
