"""Tests for Phase 14 — Registry snapshots and rollback."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from src.core.snapshot import (
    RollbackEngine,
    RollbackPlan,
    RegistrySnapshot,
    SnapshotStore,
    capture_snapshot,
)
from src.models.asset import Asset, AssetType, MigrationState


# ── Minimal registry stub ─────────────────────────────────────────────────────


class StubRegistry:
    """Minimal AssetRegistry interface used in tests."""

    def __init__(self, assets: list[Asset] | None = None):
        self._assets: dict[str, Asset] = {}
        for a in assets or []:
            self._assets[a.id] = a

    def list_assets(self) -> list[Asset]:
        return list(self._assets.values())

    def register_asset(self, asset: Asset) -> None:
        self._assets[asset.id] = asset

    def get(self, asset_id: str) -> Asset | None:
        return self._assets.get(asset_id)


def _make_asset(asset_id: str, name: str = "asset",
                state: MigrationState = MigrationState.CONVERTED,
                asset_type: AssetType = AssetType.RECIPE_SQL) -> Asset:
    return Asset(
        id=asset_id,
        type=asset_type,
        name=name,
        source_project="PROJ",
        state=state,
    )


# ── RegistrySnapshot ──────────────────────────────────────────────────────────


class TestRegistrySnapshot:
    def test_roundtrip(self):
        snap = RegistrySnapshot("s1", "PROJ", [{"id": "a1", "name": "N"}])
        restored = RegistrySnapshot.from_dict(snap.to_dict())
        assert restored.snapshot_id == "s1"
        assert restored.project_key == "PROJ"
        assert len(restored.assets) == 1

    def test_captured_at_auto_set(self):
        snap = RegistrySnapshot("s1", "P", [])
        assert snap.captured_at is not None

    def test_get_asset_found(self):
        snap = RegistrySnapshot("s1", "P", [{"id": "x", "type": "notebook"}])
        a = snap.get_asset("x")
        assert a is not None
        assert a["type"] == "notebook"

    def test_get_asset_missing(self):
        snap = RegistrySnapshot("s1", "P", [])
        assert snap.get_asset("missing") is None

    def test_assets_by_type(self):
        snap = RegistrySnapshot("s1", "P", [
            {"id": "a", "type": "recipe.sql"},
            {"id": "b", "type": "notebook"},
            {"id": "c", "type": "recipe.sql"},
        ])
        sql = snap.assets_by_type("recipe.sql")
        assert len(sql) == 2
        assert all(a["type"] == "recipe.sql" for a in sql)

    def test_to_dict_includes_asset_count(self):
        snap = RegistrySnapshot("s1", "P", [{"id": "a"}, {"id": "b"}])
        d = snap.to_dict()
        assert d["asset_count"] == 2

    def test_empty_assets(self):
        snap = RegistrySnapshot("s1", "P", [])
        assert snap.assets == []
        assert snap.get_asset("x") is None
        assert snap.assets_by_type("any") == []


# ── capture_snapshot ──────────────────────────────────────────────────────────


class TestCaptureSnapshot:
    def test_captures_all_assets(self):
        assets = [_make_asset(f"a{i}") for i in range(5)]
        reg = StubRegistry(assets)
        snap = capture_snapshot("snap-1", "PROJ", reg)
        assert len(snap.assets) == 5
        assert snap.snapshot_id == "snap-1"
        assert snap.project_key == "PROJ"

    def test_empty_registry(self):
        reg = StubRegistry()
        snap = capture_snapshot("snap-0", "PROJ", reg)
        assert snap.assets == []

    def test_asset_fields_preserved(self):
        asset = _make_asset("z1", name="ZAsset", state=MigrationState.DEPLOYED)
        snap = capture_snapshot("snap-x", "P", StubRegistry([asset]))
        assert snap.assets[0]["id"] == "z1"
        assert snap.assets[0]["name"] == "ZAsset"


# ── SnapshotStore ─────────────────────────────────────────────────────────────


class TestSnapshotStore:
    def test_save_creates_file(self, tmp_path):
        store = SnapshotStore(tmp_path)
        snap = RegistrySnapshot("snap-A", "P", [])
        path = store.save(snap)
        assert path.exists()
        assert "snap-A" in path.name

    def test_load_by_id(self, tmp_path):
        store = SnapshotStore(tmp_path)
        snap = RegistrySnapshot("snap-B", "MY_PROJ", [{"id": "x"}])
        store.save(snap)
        loaded = store.load("snap-B")
        assert loaded is not None
        assert loaded.project_key == "MY_PROJ"
        assert len(loaded.assets) == 1

    def test_load_nonexistent_returns_none(self, tmp_path):
        store = SnapshotStore(tmp_path)
        assert store.load("does-not-exist") is None

    def test_list_snapshots(self, tmp_path):
        store = SnapshotStore(tmp_path)
        store.save(RegistrySnapshot("snap-1", "P", []))
        store.save(RegistrySnapshot("snap-2", "P", []))
        ids = store.list_snapshots()
        assert "snap-1" in ids
        assert "snap-2" in ids

    def test_delete_snapshot(self, tmp_path):
        store = SnapshotStore(tmp_path)
        store.save(RegistrySnapshot("snap-del", "P", []))
        assert store.delete("snap-del") is True
        assert store.load("snap-del") is None

    def test_delete_nonexistent(self, tmp_path):
        store = SnapshotStore(tmp_path)
        assert store.delete("never-saved") is False

    def test_creates_directory(self, tmp_path):
        deep = tmp_path / "a" / "b" / "snapshots"
        store = SnapshotStore(deep)
        assert deep.exists()

    def test_multiple_saves_same_id(self, tmp_path):
        """Overwriting same snapshot_id should update the file."""
        store = SnapshotStore(tmp_path)
        store.save(RegistrySnapshot("snap-x", "P", []))
        store.save(RegistrySnapshot("snap-x", "P", [{"id": "new"}]))
        loaded = store.load("snap-x")
        assert len(loaded.assets) == 1


# ── RollbackPlan ──────────────────────────────────────────────────────────────


class TestRollbackPlan:
    def test_summary_structure(self):
        plan = RollbackPlan("snap-1", [{"id": "a"}], ["b", "c"], dry_run=True)
        s = plan.summary()
        assert s["snapshot_id"] == "snap-1"
        assert s["dry_run"] is True
        assert s["assets_to_restore"] == 1
        assert s["assets_not_in_snapshot"] == 2

    def test_dry_run_flag(self):
        plan = RollbackPlan("s", [], [], dry_run=False)
        assert plan.dry_run is False

    def test_asset_type_filter_in_summary(self):
        plan = RollbackPlan("s", [], [], asset_type_filter="recipe.sql")
        assert plan.summary()["asset_type_filter"] == "recipe.sql"


# ── RollbackEngine ────────────────────────────────────────────────────────────


class TestRollbackEnginePlan:
    def _setup(self, tmp_path, assets_in_snapshot, current_assets):
        store = SnapshotStore(tmp_path)
        snap = capture_snapshot("snap-1", "PROJ", StubRegistry(assets_in_snapshot))
        store.save(snap)
        engine = RollbackEngine(store)
        registry = StubRegistry(current_assets)
        return engine, registry

    def test_plan_missing_snapshot_returns_none(self, tmp_path):
        store = SnapshotStore(tmp_path)
        engine = RollbackEngine(store)
        plan = engine.plan("nonexistent", StubRegistry())
        assert plan is None

    def test_plan_dry_run_true(self, tmp_path):
        engine, registry = self._setup(
            tmp_path,
            [_make_asset("a1")],
            [_make_asset("a1")],
        )
        plan = engine.plan("snap-1", registry)
        assert plan is not None
        assert plan.dry_run is True

    def test_plan_identifies_all_assets(self, tmp_path):
        assets = [_make_asset(f"a{i}") for i in range(4)]
        engine, registry = self._setup(tmp_path, assets, assets)
        plan = engine.plan("snap-1", registry)
        assert len(plan.assets_to_restore) == 4

    def test_plan_identifies_new_assets_not_in_snapshot(self, tmp_path):
        """Assets added after snapshot should be flagged."""
        snap_assets = [_make_asset("a1"), _make_asset("a2")]
        current_assets = snap_assets + [_make_asset("a3_new")]
        engine, registry = self._setup(tmp_path, snap_assets, current_assets)
        plan = engine.plan("snap-1", registry)
        assert "a3_new" in plan.assets_not_in_snapshot

    def test_plan_with_type_filter(self, tmp_path):
        sql_asset = _make_asset("sql1", asset_type=AssetType.RECIPE_SQL)
        py_asset = _make_asset("py1", asset_type=AssetType.RECIPE_PYTHON)
        engine, registry = self._setup(tmp_path, [sql_asset, py_asset], [sql_asset, py_asset])
        plan = engine.plan("snap-1", registry, asset_type_filter="recipe.sql")
        assert len(plan.assets_to_restore) == 1
        assert plan.assets_to_restore[0]["type"] == "recipe.sql"

    def test_plan_empty_registry(self, tmp_path):
        engine, registry = self._setup(tmp_path, [], [])
        plan = engine.plan("snap-1", registry)
        assert plan is not None
        assert plan.assets_to_restore == []


class TestRollbackEngineApply:
    def _setup(self, tmp_path, snap_assets, current_assets):
        store = SnapshotStore(tmp_path)
        snap = capture_snapshot("snap-1", "PROJ", StubRegistry(snap_assets))
        store.save(snap)
        engine = RollbackEngine(store)
        registry = StubRegistry(current_assets)
        return engine, registry, store

    def test_apply_restores_state(self, tmp_path):
        original = _make_asset("a1", state=MigrationState.CONVERTED)
        modified = _make_asset("a1", state=MigrationState.DEPLOYED)  # post-deploy
        engine, registry, _ = self._setup(tmp_path, [original], [modified])

        plan = engine.apply("snap-1", registry)
        assert plan is not None
        assert plan.dry_run is False

        # Asset should now reflect snapshot state
        restored = registry.get("a1")
        assert restored is not None
        assert restored.state == MigrationState.CONVERTED

    def test_apply_missing_snapshot_returns_none(self, tmp_path):
        store = SnapshotStore(tmp_path)
        engine = RollbackEngine(store)
        plan = engine.apply("no-snap", StubRegistry())
        assert plan is None

    def test_apply_with_type_filter(self, tmp_path):
        sql_asset = _make_asset("sql1", state=MigrationState.CONVERTED, asset_type=AssetType.RECIPE_SQL)
        py_asset = _make_asset("py1", state=MigrationState.CONVERTED, asset_type=AssetType.RECIPE_PYTHON)

        # After deploy, both are DEPLOYED
        sql_deployed = _make_asset("sql1", state=MigrationState.DEPLOYED, asset_type=AssetType.RECIPE_SQL)
        py_deployed = _make_asset("py1", state=MigrationState.DEPLOYED, asset_type=AssetType.RECIPE_PYTHON)

        engine, registry, _ = self._setup(tmp_path, [sql_asset, py_asset], [sql_deployed, py_deployed])

        # Roll back only SQL assets
        engine.apply("snap-1", registry, asset_type_filter="recipe.sql")

        # SQL should be rolled back
        assert registry.get("sql1").state == MigrationState.CONVERTED
        # Python should remain DEPLOYED (not in filter)
        assert registry.get("py1").state == MigrationState.DEPLOYED

    def test_apply_multiple_assets(self, tmp_path):
        snap_assets = [_make_asset(f"a{i}", state=MigrationState.CONVERTED) for i in range(5)]
        deployed = [_make_asset(f"a{i}", state=MigrationState.DEPLOYED) for i in range(5)]
        engine, registry, _ = self._setup(tmp_path, snap_assets, deployed)
        plan = engine.apply("snap-1", registry)
        assert len(plan.assets_to_restore) == 5
        for asset in registry.list_assets():
            assert asset.state == MigrationState.CONVERTED

    def test_apply_does_not_delete_new_assets(self, tmp_path):
        """Safe rollback: new assets added after snapshot are NOT deleted."""
        original = _make_asset("a1")
        new_asset = _make_asset("a2_new")
        engine, registry, _ = self._setup(tmp_path, [original], [original, new_asset])
        engine.apply("snap-1", registry)
        # a2_new should still exist
        assert registry.get("a2_new") is not None


# ── Integration: snapshot → deploy → rollback cycle ──────────────────────────


class TestSnapshotRollbackCycle:
    def test_full_cycle(self, tmp_path):
        """Capture → deploy → roll back → verify restoration."""
        store = SnapshotStore(tmp_path)
        engine = RollbackEngine(store)

        initial_assets = [_make_asset(f"a{i}", state=MigrationState.CONVERTED) for i in range(3)]
        registry = StubRegistry(initial_assets)

        # 1. Capture pre-deploy snapshot
        snap = capture_snapshot("pre-deploy-1", "PROJ", registry)
        store.save(snap)

        # 2. Simulate deploy (state transitions to DEPLOYED)
        for a in registry.list_assets():
            registry.register_asset(
                Asset(id=a.id, type=a.type, name=a.name,
                      source_project=a.source_project, state=MigrationState.DEPLOYED)
            )
        assert all(a.state == MigrationState.DEPLOYED for a in registry.list_assets())

        # 3. Roll back
        plan = engine.apply("pre-deploy-1", registry)
        assert plan is not None

        # 4. Verify state restored to CONVERTED
        for a in registry.list_assets():
            assert a.state == MigrationState.CONVERTED, f"Asset {a.id} not rolled back"

    def test_dry_run_does_not_mutate(self, tmp_path):
        """plan() must never change registry state."""
        store = SnapshotStore(tmp_path)
        engine = RollbackEngine(store)

        initial = [_make_asset("x", state=MigrationState.CONVERTED)]
        snap = capture_snapshot("snap-dr", "P", StubRegistry(initial))
        store.save(snap)

        # Registry is in DEPLOYED state
        registry = StubRegistry([_make_asset("x", state=MigrationState.DEPLOYED)])

        plan = engine.plan("snap-dr", registry)
        assert plan.dry_run is True

        # State should not have changed (still DEPLOYED)
        assert registry.get("x").state == MigrationState.DEPLOYED
