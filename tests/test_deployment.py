"""Tests for Phase 12 — Deployment idempotency: content hashing and manifest."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from src.core.deployment import (
    DeploymentEntry,
    DeploymentManifest,
    DeploymentManifestStore,
    IdempotencyChecker,
    content_hash,
)


# ── content_hash ──────────────────────────────────────────────────────────────


class TestContentHash:
    def test_string_returns_64_char_hex(self):
        h = content_hash("hello world")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_bytes_input(self):
        h1 = content_hash("hello".encode())
        h2 = content_hash("hello")
        assert h1 == h2

    def test_dict_input_stable(self):
        h1 = content_hash({"b": 2, "a": 1})
        h2 = content_hash({"a": 1, "b": 2})
        assert h1 == h2, "Hash must be order-independent for dicts"

    def test_list_input(self):
        h = content_hash([1, 2, 3])
        assert len(h) == 64

    def test_different_content_different_hash(self):
        assert content_hash("version1") != content_hash("version2")

    def test_same_content_same_hash(self):
        assert content_hash("abc") == content_hash("abc")

    def test_empty_string(self):
        h = content_hash("")
        assert len(h) == 64  # valid SHA-256 of empty string

    def test_unicode_content(self):
        h1 = content_hash("Ré·pertoire")
        h2 = content_hash("Ré·pertoire")
        assert h1 == h2

    def test_nested_dict(self):
        h1 = content_hash({"outer": {"inner": [1, 2, 3]}})
        h2 = content_hash({"outer": {"inner": [1, 2, 3]}})
        assert h1 == h2

    def test_large_content(self):
        big = "x" * 100_000
        h = content_hash(big)
        assert len(h) == 64


# ── DeploymentEntry ───────────────────────────────────────────────────────────


class TestDeploymentEntry:
    def _make(self, **kwargs):
        defaults = dict(
            asset_id="asset-001",
            asset_type="notebook",
            asset_name="My Notebook",
            content_hash_value="abc123",
        )
        defaults.update(kwargs)
        return DeploymentEntry(**defaults)

    def test_roundtrip(self):
        entry = self._make(fabric_item_id="fab-42", skipped=False)
        d = entry.to_dict()
        restored = DeploymentEntry.from_dict(d)
        assert restored.asset_id == entry.asset_id
        assert restored.asset_type == entry.asset_type
        assert restored.content_hash_value == entry.content_hash_value
        assert restored.fabric_item_id == entry.fabric_item_id

    def test_deployed_at_auto_set(self):
        entry = self._make()
        assert entry.deployed_at is not None

    def test_skipped_flag_preserved(self):
        entry = self._make(skipped=True)
        assert DeploymentEntry.from_dict(entry.to_dict()).skipped is True

    def test_default_not_skipped(self):
        entry = self._make()
        assert entry.skipped is False


# ── DeploymentManifest ────────────────────────────────────────────────────────


class TestDeploymentManifest:
    def _make_manifest(self) -> DeploymentManifest:
        return DeploymentManifest("run-001", "ECOMM_PROJ")

    def test_empty_manifest(self):
        m = self._make_manifest()
        assert m.deployed_count == 0
        assert m.skipped_count == 0
        assert m.total_count == 0

    def test_add_deployed_entry(self):
        m = self._make_manifest()
        m.add_entry(DeploymentEntry("a1", "notebook", "NB", "hash1", skipped=False))
        assert m.deployed_count == 1
        assert m.skipped_count == 0
        assert m.total_count == 1

    def test_add_skipped_entry(self):
        m = self._make_manifest()
        m.add_entry(DeploymentEntry("a2", "pipeline", "PL", "hash2", skipped=True))
        assert m.deployed_count == 0
        assert m.skipped_count == 1

    def test_mixed_entries_counts(self):
        m = self._make_manifest()
        for i in range(3):
            m.add_entry(DeploymentEntry(f"d{i}", "sql", f"SQL{i}", f"h{i}", skipped=False))
        for i in range(2):
            m.add_entry(DeploymentEntry(f"s{i}", "sql", f"SKIP{i}", f"h{i}", skipped=True))
        assert m.deployed_count == 3
        assert m.skipped_count == 2
        assert m.total_count == 5

    def test_mark_finished_sets_timestamp(self):
        m = self._make_manifest()
        assert m.finished_at is None
        m.mark_finished()
        assert m.finished_at is not None
        assert "T" in m.finished_at  # ISO-8601

    def test_roundtrip(self):
        m = self._make_manifest()
        m.add_entry(DeploymentEntry("x", "notebook", "NB", "abc", fabric_item_id="f1"))
        m.mark_finished()
        d = m.to_dict()
        restored = DeploymentManifest.from_dict(d)
        assert restored.run_id == m.run_id
        assert restored.project_key == m.project_key
        assert restored.finished_at == m.finished_at
        entry = restored.get_entry("x")
        assert entry is not None
        assert entry.fabric_item_id == "f1"

    def test_overwrite_entry(self):
        m = self._make_manifest()
        m.add_entry(DeploymentEntry("id1", "sql", "T", "hash1"))
        m.add_entry(DeploymentEntry("id1", "sql", "T", "hash2"))  # overwrite
        assert m.total_count == 1
        assert m.get_entry("id1").content_hash_value == "hash2"

    def test_get_entry_missing_returns_none(self):
        m = self._make_manifest()
        assert m.get_entry("nonexistent") is None


# ── DeploymentManifestStore ───────────────────────────────────────────────────


class TestDeploymentManifestStore:
    def test_save_creates_file(self, tmp_path):
        store = DeploymentManifestStore(tmp_path)
        m = DeploymentManifest("run-XYZ", "PROJ")
        path = store.save(m)
        assert path.exists()
        assert "run-XYZ" in path.name

    def test_latest_json_created(self, tmp_path):
        store = DeploymentManifestStore(tmp_path)
        m = DeploymentManifest("run-1", "PROJ")
        store.save(m)
        assert (tmp_path / "latest.json").exists()

    def test_load_by_run_id(self, tmp_path):
        store = DeploymentManifestStore(tmp_path)
        m = DeploymentManifest("run-999", "MY_PROJ")
        m.add_entry(DeploymentEntry("a1", "nb", "N", "hh"))
        store.save(m)

        loaded = store.load("run-999")
        assert loaded is not None
        assert loaded.run_id == "run-999"
        assert loaded.project_key == "MY_PROJ"
        assert loaded.total_count == 1

    def test_load_nonexistent_returns_none(self, tmp_path):
        store = DeploymentManifestStore(tmp_path)
        assert store.load("does-not-exist") is None

    def test_load_latest(self, tmp_path):
        store = DeploymentManifestStore(tmp_path)
        store.save(DeploymentManifest("run-1", "P"))
        store.save(DeploymentManifest("run-2", "P"))
        latest = store.load_latest()
        assert latest is not None
        assert latest.run_id == "run-2"

    def test_load_latest_empty_store(self, tmp_path):
        store = DeploymentManifestStore(tmp_path)
        assert store.load_latest() is None

    def test_list_runs(self, tmp_path):
        store = DeploymentManifestStore(tmp_path)
        store.save(DeploymentManifest("run-a", "P"))
        store.save(DeploymentManifest("run-b", "P"))
        runs = store.list_runs()
        assert "run-a" in runs
        assert "run-b" in runs

    def test_creates_manifest_dir(self, tmp_path):
        deep = tmp_path / "a" / "b" / "manifests"
        store = DeploymentManifestStore(deep)
        assert deep.exists()

    def test_manifest_json_is_valid(self, tmp_path):
        store = DeploymentManifestStore(tmp_path)
        m = DeploymentManifest("run-json", "P")
        m.add_entry(DeploymentEntry("x", "sql", "T", "hh", fabric_item_id="fab1"))
        m.mark_finished()
        path = store.save(m)
        parsed = json.loads(path.read_text())
        assert parsed["run_id"] == "run-json"
        assert parsed["deployed_count"] == 1
        assert parsed["entries"]["x"]["fabric_item_id"] == "fab1"


# ── IdempotencyChecker ────────────────────────────────────────────────────────


class TestIdempotencyChecker:
    def _make_checker(self, tmp_path, run_id="run-001", project_key="PROJ"):
        store = DeploymentManifestStore(tmp_path)
        return IdempotencyChecker(store, run_id, project_key)

    def test_new_asset_needs_deploy(self, tmp_path):
        checker = self._make_checker(tmp_path)
        checker.load_previous()
        assert checker.needs_deploy("brand-new", "CREATE TABLE ...") is True

    def test_unchanged_asset_no_deploy(self, tmp_path):
        store = DeploymentManifestStore(tmp_path)
        # Simulate a previous run
        prev = DeploymentManifest("run-000", "PROJ")
        content = "SELECT * FROM orders"
        prev.add_entry(DeploymentEntry("asset-1", "sql", "T", content_hash(content)))
        store.save(prev)

        checker = IdempotencyChecker(store, "run-001", "PROJ")
        checker.load_previous()
        assert checker.needs_deploy("asset-1", content) is False

    def test_changed_asset_needs_deploy(self, tmp_path):
        store = DeploymentManifestStore(tmp_path)
        prev = DeploymentManifest("run-000", "PROJ")
        prev.add_entry(DeploymentEntry("asset-1", "sql", "T", content_hash("old content")))
        store.save(prev)

        checker = IdempotencyChecker(store, "run-001", "PROJ")
        checker.load_previous()
        assert checker.needs_deploy("asset-1", "new content") is True

    def test_record_deployed_added_to_manifest(self, tmp_path):
        checker = self._make_checker(tmp_path)
        checker.load_previous()
        checker.record_deployed("a1", "notebook", "NB", "content", "fab-id")
        assert checker.current_manifest.deployed_count == 1
        assert checker.current_manifest.get_entry("a1").fabric_item_id == "fab-id"

    def test_record_skipped(self, tmp_path):
        checker = self._make_checker(tmp_path)
        checker.load_previous()
        checker.record_skipped("a2", "sql", "T", "content")
        assert checker.current_manifest.skipped_count == 1
        assert checker.current_manifest.get_entry("a2").skipped is True

    def test_finalize_saves_manifest(self, tmp_path):
        store = DeploymentManifestStore(tmp_path)
        checker = IdempotencyChecker(store, "run-fin", "PROJ")
        checker.load_previous()
        checker.record_deployed("x", "nb", "N", "data")
        path = checker.finalize()
        assert path.exists()
        assert store.load("run-fin") is not None

    def test_finalize_sets_finished_at(self, tmp_path):
        checker = self._make_checker(tmp_path, run_id="run-ft")
        checker.load_previous()
        checker.finalize()
        loaded = DeploymentManifestStore(tmp_path).load("run-ft")
        assert loaded.finished_at is not None

    def test_no_previous_manifest_loads_zero(self, tmp_path):
        checker = self._make_checker(tmp_path)
        count = checker.load_previous()
        assert count == 0

    def test_full_idempotency_cycle(self, tmp_path):
        """Asset deployed in run-1 should be skipped in run-2 if unchanged."""
        store = DeploymentManifestStore(tmp_path)
        content = "CREATE TABLE orders (id INT)"

        # Run 1: deploy
        c1 = IdempotencyChecker(store, "run-1", "PROJ")
        c1.load_previous()
        assert c1.needs_deploy("tbl", content) is True
        c1.record_deployed("tbl", "ddl", "orders", content, "fab-1")
        c1.finalize()

        # Run 2: no change → skip
        c2 = IdempotencyChecker(store, "run-2", "PROJ")
        c2.load_previous()
        assert c2.needs_deploy("tbl", content) is False

        # Run 3: content changed → re-deploy
        c2.finalize()
        c3 = IdempotencyChecker(store, "run-3", "PROJ")
        c3.load_previous()
        assert c3.needs_deploy("tbl", "CREATE TABLE orders (id INT, ts TIMESTAMP)") is True

    def test_skipped_asset_not_in_previous_hashes(self, tmp_path):
        """Skipped entries don't update the hash baseline."""
        store = DeploymentManifestStore(tmp_path)
        content = "SELECT 1"

        c1 = IdempotencyChecker(store, "run-1", "PROJ")
        c1.load_previous()
        c1.record_skipped("x", "sql", "T", content)  # skipped, no actual deploy
        c1.finalize()

        # In next run, skipped asset is still "new" (was never truly deployed)
        c2 = IdempotencyChecker(store, "run-2", "PROJ")
        c2.load_previous()
        assert c2.needs_deploy("x", content) is True
