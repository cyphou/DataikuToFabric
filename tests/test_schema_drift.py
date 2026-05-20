"""Tests for Phase 22 — Schema Drift Detection."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.drift.snapshot import (
    AssetSnapshot,
    ColumnSnapshot,
    MigrationSnapshot,
    take_snapshot,
)
from src.drift.drift_detector import (
    DriftItem,
    DriftReport,
    DriftSeverity,
    DriftType,
    detect_drift,
)
from src.reports.drift_report import generate_drift_html, save_drift_report
from src.models.asset import Asset, AssetType, MigrationState
from src.core.registry import AssetRegistry


# ── Snapshot ──────────────────────────────────────────────────

class TestSnapshot:
    def test_take_snapshot_empty(self, registry):
        snap = take_snapshot(registry)
        assert isinstance(snap, MigrationSnapshot)
        assert snap.project_key == "TEST_PROJECT"
        assert len(snap.assets) == 0

    def test_take_snapshot_with_assets(self, populated_registry):
        snap = take_snapshot(populated_registry)
        assert len(snap.assets) > 0
        assert all(isinstance(a, AssetSnapshot) for a in snap.assets)

    def test_snapshot_save_load(self, tmp_path, populated_registry):
        snap = take_snapshot(populated_registry)
        path = snap.save(tmp_path / "snap.json")
        assert path.exists()

        loaded = MigrationSnapshot.load(path)
        assert loaded.snapshot_id == snap.snapshot_id
        assert len(loaded.assets) == len(snap.assets)

    def test_snapshot_to_dict(self, populated_registry):
        snap = take_snapshot(populated_registry)
        d = snap.to_dict()
        assert "snapshot_id" in d
        assert "assets" in d
        assert "timestamp" in d

    def test_column_snapshot(self):
        cs = ColumnSnapshot(name="id", data_type="int", nullable=False)
        assert cs.name == "id"
        assert cs.data_type == "int"
        assert cs.nullable is False


# ── Drift Detection ──────────────────────────────────────────

class TestDriftDetector:
    def test_detect_no_drift(self, populated_registry):
        snap_a = take_snapshot(populated_registry)
        snap_b = take_snapshot(populated_registry)
        report = detect_drift(snap_a, snap_b)
        assert isinstance(report, DriftReport)
        assert report.has_breaking_changes is False

    def test_detect_asset_added(self, populated_registry, tmp_path):
        empty_reg = AssetRegistry(project_key="TEST_PROJECT", registry_path=tmp_path / "empty_reg.json")
        snap_base = take_snapshot(empty_reg)
        snap_current = take_snapshot(populated_registry)
        report = detect_drift(snap_base, snap_current)
        assert len(report.drifts) > 0
        added = [i for i in report.drifts if i.drift_type == DriftType.ASSET_ADDED]
        assert len(added) > 0

    def test_detect_asset_removed(self, populated_registry, tmp_path):
        empty_reg = AssetRegistry(project_key="TEST_PROJECT", registry_path=tmp_path / "empty_reg2.json")
        snap_base = take_snapshot(populated_registry)
        snap_current = take_snapshot(empty_reg)
        report = detect_drift(snap_base, snap_current)
        removed = [i for i in report.drifts if i.drift_type == DriftType.ASSET_REMOVED]
        assert len(removed) > 0

    def test_detect_state_change(self, populated_registry):
        snap_base = take_snapshot(populated_registry)
        # Change state of an asset
        assets = populated_registry.get_all()
        populated_registry.update_state(assets[0].id, MigrationState.CONVERTING)
        snap_current = take_snapshot(populated_registry)
        report = detect_drift(snap_base, snap_current)
        state_changes = [i for i in report.drifts if i.drift_type == DriftType.STATE_CHANGED]
        assert len(state_changes) > 0

    def test_drift_severity_breaking(self):
        item = DriftItem(
            drift_type=DriftType.COLUMN_REMOVED,
            severity=DriftSeverity.BREAKING,
            asset_name="orders",
            description="Column removed",
        )
        assert item.severity == DriftSeverity.BREAKING

    def test_report_get_by_severity(self, populated_registry, registry):
        snap_base = take_snapshot(registry)
        snap_current = take_snapshot(populated_registry)
        report = detect_drift(snap_base, snap_current)
        info_items = report.get_by_severity(DriftSeverity.INFO)
        assert isinstance(info_items, list)

    def test_report_to_dict(self, populated_registry, registry):
        snap_base = take_snapshot(registry)
        snap_current = take_snapshot(populated_registry)
        report = detect_drift(snap_base, snap_current)
        d = report.to_dict()
        assert "drifts" in d
        assert "has_breaking_changes" in d


# ── Drift Report ──────────────────────────────────────────────

class TestDriftReport:
    def test_generate_html(self, populated_registry, registry):
        snap_base = take_snapshot(registry)
        snap_current = take_snapshot(populated_registry)
        report = detect_drift(snap_base, snap_current)
        html = generate_drift_html(report)
        assert "<html" in html

    def test_save_report(self, tmp_path, populated_registry, registry):
        snap_base = take_snapshot(registry)
        snap_current = take_snapshot(populated_registry)
        report = detect_drift(snap_base, snap_current)
        path = save_drift_report(report, tmp_path / "drift.html")
        assert path.exists()
