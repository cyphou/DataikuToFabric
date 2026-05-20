"""Plugin manager — discovers, loads, and dispatches hooks to plugins."""

from __future__ import annotations

import importlib
import importlib.util
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.plugins.base_plugin import BasePlugin


@dataclass
class PluginInfo:
    """Information about a loaded plugin."""
    name: str
    version: str
    description: str
    module_path: str


class PluginManager:
    """Manages plugin lifecycle: discovery, loading, and hook dispatch."""

    def __init__(self) -> None:
        self._plugins: list[BasePlugin] = []

    @property
    def plugins(self) -> list[BasePlugin]:
        return list(self._plugins)

    def register(self, plugin: BasePlugin) -> None:
        """Register a plugin instance."""
        # Avoid duplicates
        if any(p.name == plugin.name for p in self._plugins):
            return
        self._plugins.append(plugin)

    def unregister(self, plugin_name: str) -> bool:
        """Unregister a plugin by name."""
        before = len(self._plugins)
        self._plugins = [p for p in self._plugins if p.name != plugin_name]
        return len(self._plugins) < before

    def load_from_directory(self, directory: str | Path) -> list[PluginInfo]:
        """Load all plugins from a directory.

        Each .py file in the directory is imported, and any BasePlugin
        subclasses found are instantiated and registered.

        Args:
            directory: Path to plugins directory.

        Returns:
            List of PluginInfo for loaded plugins.
        """
        loaded: list[PluginInfo] = []
        dir_path = Path(directory)

        if not dir_path.is_dir():
            return loaded

        for py_file in sorted(dir_path.glob("*.py")):
            if py_file.name.startswith("_"):
                continue

            module_name = f"plugins.{py_file.stem}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, py_file)
                if spec is None or spec.loader is None:
                    continue
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)

                # Find BasePlugin subclasses
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (
                        isinstance(attr, type)
                        and issubclass(attr, BasePlugin)
                        and attr is not BasePlugin
                    ):
                        instance = attr()
                        self.register(instance)
                        loaded.append(PluginInfo(
                            name=instance.name,
                            version=instance.version,
                            description=instance.description,
                            module_path=str(py_file),
                        ))
            except Exception:
                # Skip plugins that fail to load
                continue

        return loaded

    def list_plugins(self) -> list[PluginInfo]:
        """List all registered plugins."""
        return [
            PluginInfo(
                name=p.name,
                version=p.version,
                description=p.description,
                module_path="",
            )
            for p in self._plugins
        ]

    # ── Hook dispatchers ────────────────────────────────

    def dispatch_discovery_complete(self, assets: list[Any], context: dict[str, Any]) -> None:
        for plugin in self._plugins:
            plugin.on_discovery_complete(assets, context)

    def dispatch_before_convert(self, asset: Any, context: dict[str, Any]) -> Any:
        current = asset
        for plugin in self._plugins:
            result = plugin.on_before_convert(current, context)
            if result is not None:
                current = result
        return current

    def dispatch_after_convert(self, asset: Any, result: Any, context: dict[str, Any]) -> None:
        for plugin in self._plugins:
            plugin.on_after_convert(asset, result, context)

    def dispatch_before_validate(self, assets: list[Any], context: dict[str, Any]) -> None:
        for plugin in self._plugins:
            plugin.on_before_validate(assets, context)

    def dispatch_after_validate(self, assets: list[Any], results: Any, context: dict[str, Any]) -> None:
        for plugin in self._plugins:
            plugin.on_after_validate(assets, results, context)

    def dispatch_before_deploy(self, assets: list[Any], context: dict[str, Any]) -> None:
        for plugin in self._plugins:
            plugin.on_before_deploy(assets, context)

    def dispatch_after_deploy(self, assets: list[Any], results: Any, context: dict[str, Any]) -> None:
        for plugin in self._plugins:
            plugin.on_after_deploy(assets, results, context)
