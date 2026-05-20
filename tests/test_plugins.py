"""Tests for Phase 25 — Plugin System."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.plugins.base_plugin import BasePlugin
from src.plugins.plugin_manager import PluginInfo, PluginManager


# ── Test plugin for direct registration ───────────────────────

class DummyPlugin(BasePlugin):
    @property
    def name(self) -> str:
        return "dummy"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "Test plugin"

    def __init__(self):
        self.calls: list[str] = []

    def on_discovery_complete(self, assets, context):
        self.calls.append("discovery_complete")

    def on_before_convert(self, asset, context):
        self.calls.append("before_convert")
        return asset

    def on_after_convert(self, asset, result, context):
        self.calls.append("after_convert")

    def on_before_validate(self, assets, context):
        self.calls.append("before_validate")

    def on_after_validate(self, assets, results, context):
        self.calls.append("after_validate")

    def on_before_deploy(self, assets, context):
        self.calls.append("before_deploy")

    def on_after_deploy(self, assets, results, context):
        self.calls.append("after_deploy")


# ── Plugin Manager ────────────────────────────────────────────

class TestPluginManager:
    def test_register_plugin(self):
        pm = PluginManager()
        pm.register(DummyPlugin())
        assert len(pm.plugins) == 1

    def test_register_duplicate_ignored(self):
        pm = PluginManager()
        pm.register(DummyPlugin())
        pm.register(DummyPlugin())
        assert len(pm.plugins) == 1

    def test_unregister_plugin(self):
        pm = PluginManager()
        pm.register(DummyPlugin())
        assert pm.unregister("dummy") is True
        assert len(pm.plugins) == 0

    def test_unregister_nonexistent(self):
        pm = PluginManager()
        assert pm.unregister("nope") is False

    def test_list_plugins(self):
        pm = PluginManager()
        pm.register(DummyPlugin())
        plugins = pm.list_plugins()
        assert len(plugins) == 1
        assert plugins[0].name == "dummy"

    def test_dispatch_discovery_complete(self):
        pm = PluginManager()
        dp = DummyPlugin()
        pm.register(dp)
        pm.dispatch_discovery_complete(["asset1"], {})
        assert "discovery_complete" in dp.calls

    def test_dispatch_before_convert(self):
        pm = PluginManager()
        dp = DummyPlugin()
        pm.register(dp)
        result = pm.dispatch_before_convert("asset1", {})
        assert result == "asset1"
        assert "before_convert" in dp.calls

    def test_dispatch_after_convert(self):
        pm = PluginManager()
        dp = DummyPlugin()
        pm.register(dp)
        pm.dispatch_after_convert("asset1", "result1", {})
        assert "after_convert" in dp.calls

    def test_dispatch_before_validate(self):
        pm = PluginManager()
        dp = DummyPlugin()
        pm.register(dp)
        pm.dispatch_before_validate(["a1"], {})
        assert "before_validate" in dp.calls

    def test_dispatch_after_validate(self):
        pm = PluginManager()
        dp = DummyPlugin()
        pm.register(dp)
        pm.dispatch_after_validate(["a1"], "results", {})
        assert "after_validate" in dp.calls

    def test_dispatch_before_deploy(self):
        pm = PluginManager()
        dp = DummyPlugin()
        pm.register(dp)
        pm.dispatch_before_deploy(["a1"], {})
        assert "before_deploy" in dp.calls

    def test_dispatch_after_deploy(self):
        pm = PluginManager()
        dp = DummyPlugin()
        pm.register(dp)
        pm.dispatch_after_deploy(["a1"], "results", {})
        assert "after_deploy" in dp.calls

    def test_multiple_plugins_chain(self):
        pm = PluginManager()
        dp1 = DummyPlugin()
        # Create a second plugin with different name
        class OtherPlugin(BasePlugin):
            @property
            def name(self):
                return "other"
            @property
            def version(self):
                return "1.0"
            def on_before_convert(self, asset, context):
                return f"modified_{asset}"

        pm.register(dp1)
        pm.register(OtherPlugin())
        result = pm.dispatch_before_convert("asset1", {})
        assert result == "modified_asset1"


# ── Load from directory ───────────────────────────────────────

class TestPluginLoading:
    def test_load_from_directory(self):
        pm = PluginManager()
        # Load from the actual plugins/ directory
        plugins_dir = Path(__file__).parent.parent / "plugins"
        if plugins_dir.is_dir():
            loaded = pm.load_from_directory(plugins_dir)
            assert len(loaded) >= 1
            assert any(pi.name == "sample_plugin" for pi in loaded)

    def test_load_from_nonexistent_dir(self):
        pm = PluginManager()
        loaded = pm.load_from_directory("/nonexistent/path")
        assert loaded == []

    def test_load_from_empty_dir(self, tmp_path):
        pm = PluginManager()
        loaded = pm.load_from_directory(tmp_path)
        assert loaded == []


# ── Sample Plugin ─────────────────────────────────────────────

class TestSamplePlugin:
    def test_sample_plugin_hooks(self):
        from plugins.sample_plugin import SamplePlugin
        sp = SamplePlugin()
        assert sp.name == "sample_plugin"
        assert sp.version == "1.0.0"

        sp.on_discovery_complete(["a", "b", "c"], {})
        assert len(sp.events) == 1
        assert "3 assets" in sp.events[0]

    def test_sample_plugin_convert_hooks(self):
        from plugins.sample_plugin import SamplePlugin

        class FakeAsset:
            name = "test"

        sp = SamplePlugin()
        result = sp.on_before_convert(FakeAsset(), {})
        assert result is not None
        sp.on_after_convert(FakeAsset(), None, {})
        assert len(sp.events) == 2
