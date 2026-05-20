"""Sample plugin — demonstrates the plugin hook interface."""

from __future__ import annotations

from typing import Any

from src.plugins.base_plugin import BasePlugin


class SamplePlugin(BasePlugin):
    """A sample plugin that logs lifecycle events."""

    @property
    def name(self) -> str:
        return "sample_plugin"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Sample plugin demonstrating hook points"

    def __init__(self) -> None:
        self.events: list[str] = []

    def on_discovery_complete(self, assets: list[Any], context: dict[str, Any]) -> None:
        self.events.append(f"discovery_complete: {len(assets)} assets")

    def on_before_convert(self, asset: Any, context: dict[str, Any]) -> Any:
        name = getattr(asset, "name", str(asset))
        self.events.append(f"before_convert: {name}")
        return asset

    def on_after_convert(self, asset: Any, result: Any, context: dict[str, Any]) -> None:
        name = getattr(asset, "name", str(asset))
        self.events.append(f"after_convert: {name}")

    def on_before_validate(self, assets: list[Any], context: dict[str, Any]) -> None:
        self.events.append(f"before_validate: {len(assets)} assets")

    def on_after_validate(self, assets: list[Any], results: Any, context: dict[str, Any]) -> None:
        self.events.append(f"after_validate: {len(assets)} assets")

    def on_before_deploy(self, assets: list[Any], context: dict[str, Any]) -> None:
        self.events.append(f"before_deploy: {len(assets)} assets")

    def on_after_deploy(self, assets: list[Any], results: Any, context: dict[str, Any]) -> None:
        self.events.append(f"after_deploy: {len(assets)} assets")
