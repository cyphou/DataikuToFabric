"""Tests for Phase 27 — Multi-Project Merge & Shared Models."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.merge.merge_config import ConflictResolution, MergeConfig
from src.merge.merge_assessment import (
    AssetFingerprint,
    MergeAssessment,
    OverlapPair,
    assess_merge,
)
from src.merge.deduplicator import (
    DeduplicationResult,
    deduplicate,
)
from src.reports.merge_report import generate_merge_html, save_merge_report
from src.core.registry import AssetRegistry
from src.models.asset import Asset, AssetType, MigrationState


# ── Merge Config ──────────────────────────────────────────────

class TestMergeConfig:
    def test_defaults(self):
        cfg = MergeConfig()
        assert cfg.target_workspace == "merged"
        assert cfg.conflict_resolution == ConflictResolution.RENAME
        assert cfg.prefix_with_project is True
        assert cfg.deduplicate_datasets is True
        assert 0.0 <= cfg.similarity_threshold <= 1.0

    def test_custom_config(self):
        cfg = MergeConfig(
            target_workspace="custom",
            conflict_resolution=ConflictResolution.KEEP_FIRST,
            similarity_threshold=0.9,
        )
        assert cfg.target_workspace == "custom"
        assert cfg.conflict_resolution == ConflictResolution.KEEP_FIRST

    def test_conflict_resolution_values(self):
        assert ConflictResolution.KEEP_FIRST.value == "keep_first"
        assert ConflictResolution.KEEP_LATEST.value == "keep_latest"
        assert ConflictResolution.RENAME.value == "rename"
        assert ConflictResolution.MANUAL.value == "manual"


# ── Merge Assessment ─────────────────────────────────────────

class TestMergeAssessment:
    @pytest.fixture
    def two_registries(self, tmp_path):
        reg1 = AssetRegistry(project_key="PROJECT_A", registry_path=tmp_path / "r1.json")
        reg1.add_asset(Asset(
            id="ds_orders_a", name="orders", type=AssetType.DATASET, source_project="PROJECT_A",
            metadata={"schema": [{"name": "id", "type": "int"}]},
        ))
        reg1.add_asset(Asset(
            id="r_sql_a", name="compute", type=AssetType.RECIPE_SQL, source_project="PROJECT_A",
            metadata={"payload": "SELECT * FROM orders"},
        ))

        reg2 = AssetRegistry(project_key="PROJECT_B", registry_path=tmp_path / "r2.json")
        reg2.add_asset(Asset(
            id="ds_orders_b", name="orders", type=AssetType.DATASET, source_project="PROJECT_B",
            metadata={"schema": [{"name": "id", "type": "int"}]},
        ))
        reg2.add_asset(Asset(
            id="ds_customers_b", name="customers", type=AssetType.DATASET, source_project="PROJECT_B",
            metadata={"schema": [{"name": "cid", "type": "int"}]},
        ))

        return reg1, reg2

    def test_assess_empty(self, tmp_path):
        result = assess_merge([])
        assert result.total_assets == 0
        assert result.merge_score == 100.0

    def test_assess_single_project(self, tmp_path):
        reg = AssetRegistry(project_key="P", registry_path=tmp_path / "r.json")
        reg.add_asset(Asset(id="d1", name="ds1", type=AssetType.DATASET, source_project="P"))
        result = assess_merge([reg])
        assert result.total_assets == 1
        assert len(result.overlaps) == 0

    def test_assess_two_projects_with_overlap(self, two_registries):
        reg1, reg2 = two_registries
        result = assess_merge([reg1, reg2])
        assert result.total_assets == 4
        assert len(result.project_keys) == 2
        # Should detect overlap between orders datasets (same schema)
        assert len(result.overlaps) >= 1

    def test_assess_overlap_matrix(self, two_registries):
        reg1, reg2 = two_registries
        result = assess_merge([reg1, reg2])
        assert "PROJECT_A" in result.overlap_matrix
        assert "PROJECT_B" in result.overlap_matrix

    def test_assessment_to_dict(self, two_registries):
        reg1, reg2 = two_registries
        result = assess_merge([reg1, reg2])
        d = result.to_dict()
        assert "merge_score" in d
        assert "overlaps" in d
        assert "project_keys" in d


# ── Deduplicator ─────────────────────────────────────────────

class TestDeduplicator:
    @pytest.fixture
    def overlap_setup(self, tmp_path):
        reg1 = AssetRegistry(project_key="PA", registry_path=tmp_path / "r1.json")
        reg1.add_asset(Asset(
            id="d1", name="orders", type=AssetType.DATASET, source_project="PA",
            metadata={"schema": [{"name": "id", "type": "int"}]},
        ))

        reg2 = AssetRegistry(project_key="PB", registry_path=tmp_path / "r2.json")
        reg2.add_asset(Asset(
            id="d2", name="orders", type=AssetType.DATASET, source_project="PB",
            metadata={"schema": [{"name": "id", "type": "int"}]},
        ))

        assessment = assess_merge([reg1, reg2])
        return [reg1, reg2], assessment

    def test_deduplicate_keep_first(self, overlap_setup):
        registries, assessment = overlap_setup
        config = MergeConfig(conflict_resolution=ConflictResolution.KEEP_FIRST)
        result = deduplicate(registries, assessment, config)
        assert result.total_input_assets == 2
        assert result.total_output_assets <= 2

    def test_deduplicate_rename(self, overlap_setup):
        registries, assessment = overlap_setup
        config = MergeConfig(conflict_resolution=ConflictResolution.RENAME)
        result = deduplicate(registries, assessment, config)
        assert isinstance(result, DeduplicationResult)

    def test_deduplicate_manual(self, overlap_setup):
        registries, assessment = overlap_setup
        config = MergeConfig(conflict_resolution=ConflictResolution.MANUAL)
        result = deduplicate(registries, assessment, config)
        if assessment.overlaps:
            assert result.conflicts_pending > 0

    def test_dedup_result_to_dict(self, overlap_setup):
        registries, assessment = overlap_setup
        config = MergeConfig()
        result = deduplicate(registries, assessment, config)
        d = result.to_dict()
        assert "total_input" in d
        assert "total_output" in d

    def test_dedup_prefix_with_project(self, overlap_setup):
        registries, assessment = overlap_setup
        config = MergeConfig(prefix_with_project=True)
        result = deduplicate(registries, assessment, config)
        for ma in result.merged_assets:
            assert "__" in ma["merged_name"]


# ── Merge Report ──────────────────────────────────────────────

class TestMergeReport:
    def test_generate_html(self, tmp_path):
        reg1 = AssetRegistry(project_key="A", registry_path=tmp_path / "r1.json")
        reg1.add_asset(Asset(id="d1", name="d1", type=AssetType.DATASET, source_project="A"))
        reg2 = AssetRegistry(project_key="B", registry_path=tmp_path / "r2.json")
        reg2.add_asset(Asset(id="d2", name="d2", type=AssetType.DATASET, source_project="B"))
        assessment = assess_merge([reg1, reg2])
        html = generate_merge_html(assessment)
        assert "<html" in html
        assert "Merge" in html

    def test_save_report(self, tmp_path):
        assessment = MergeAssessment(project_keys=["A", "B"], total_assets=5)
        path = save_merge_report(assessment, tmp_path / "merge.html")
        assert path.exists()

    def test_report_with_dedup(self, tmp_path):
        assessment = MergeAssessment(project_keys=["A", "B"], total_assets=5)
        dedup = DeduplicationResult(
            total_input_assets=5, total_output_assets=4,
            conflicts_resolved=1, conflicts_pending=0,
        )
        html = generate_merge_html(assessment, dedup)
        assert "Deduplication" in html
