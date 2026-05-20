"""Tests for Phase 26 — Equivalence Testing & Regression Suite."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.testing.equivalence_tester import (
    ColumnComparison,
    EquivalenceResult,
    EquivalenceStatus,
    EquivalenceSuite,
    run_equivalence_suite,
    test_equivalence as check_equivalence,
)
from src.testing.regression_suite import (
    RegressionCase,
    RegressionSuite,
    create_baseline,
    run_regression,
)
from src.models.asset import Asset, AssetType, MigrationState


# ── Equivalence Tester ────────────────────────────────────────

class TestEquivalenceTester:
    def test_non_dataset_untestable(self):
        asset = Asset(id="r1", name="recipe", type=AssetType.RECIPE_SQL, source_project="P")
        result = check_equivalence(asset)
        assert result.status == EquivalenceStatus.UNTESTABLE

    def test_no_target_untestable(self):
        asset = Asset(id="d1", name="ds", type=AssetType.DATASET, source_project="P")
        result = check_equivalence(asset)
        assert result.status == EquivalenceStatus.UNTESTABLE

    def test_no_schema_untestable(self):
        asset = Asset(
            id="d1", name="ds", type=AssetType.DATASET, source_project="P",
            metadata={},
            target_fabric_asset={},
        )
        result = check_equivalence(asset)
        assert result.status == EquivalenceStatus.UNTESTABLE

    def test_equivalent_schemas(self):
        asset = Asset(
            id="d1", name="ds", type=AssetType.DATASET, source_project="P",
            metadata={
                "schema": [{"name": "id", "type": "int"}, {"name": "name", "type": "varchar"}],
                "row_count": 100,
            },
            target_fabric_asset={
                "schema": [{"name": "id", "type": "int"}, {"name": "name", "type": "varchar"}],
                "row_count": 100,
            },
        )
        result = check_equivalence(asset)
        assert result.status == EquivalenceStatus.EQUIVALENT
        assert result.row_count_match is True

    def test_different_row_counts(self):
        asset = Asset(
            id="d1", name="ds", type=AssetType.DATASET, source_project="P",
            metadata={"schema": [{"name": "id", "type": "int"}], "row_count": 100},
            target_fabric_asset={"schema": [{"name": "id", "type": "int"}], "row_count": 50},
        )
        result = check_equivalence(asset)
        assert result.status == EquivalenceStatus.DIFFERENT
        assert result.row_count_match is False

    def test_missing_column_in_target(self):
        asset = Asset(
            id="d1", name="ds", type=AssetType.DATASET, source_project="P",
            metadata={"schema": [{"name": "id", "type": "int"}, {"name": "extra", "type": "varchar"}]},
            target_fabric_asset={"schema": [{"name": "id", "type": "int"}]},
        )
        result = check_equivalence(asset)
        assert result.status == EquivalenceStatus.DIFFERENT
        assert any("missing" in f.lower() for f in result.findings)

    def test_type_mismatch(self):
        asset = Asset(
            id="d1", name="ds", type=AssetType.DATASET, source_project="P",
            metadata={"schema": [{"name": "id", "type": "int"}]},
            target_fabric_asset={"schema": [{"name": "id", "type": "varchar"}]},
        )
        result = check_equivalence(asset)
        assert result.status == EquivalenceStatus.DIFFERENT

    def test_compatible_types(self):
        asset = Asset(
            id="d1", name="ds", type=AssetType.DATASET, source_project="P",
            metadata={"schema": [{"name": "id", "type": "NUMBER"}]},
            target_fabric_asset={"schema": [{"name": "id", "type": "DECIMAL"}]},
        )
        result = check_equivalence(asset)
        assert result.status == EquivalenceStatus.EQUIVALENT

    def test_extra_target_column(self):
        asset = Asset(
            id="d1", name="ds", type=AssetType.DATASET, source_project="P",
            metadata={"schema": [{"name": "id", "type": "int"}]},
            target_fabric_asset={"schema": [{"name": "id", "type": "int"}, {"name": "extra", "type": "varchar"}]},
        )
        result = check_equivalence(asset)
        assert any("extra" in f.lower() for f in result.findings)

    def test_result_to_dict(self):
        asset = Asset(
            id="d1", name="ds", type=AssetType.DATASET, source_project="P",
            metadata={"schema": [{"name": "id", "type": "int"}]},
            target_fabric_asset={"schema": [{"name": "id", "type": "int"}]},
        )
        result = check_equivalence(asset)
        d = result.to_dict()
        assert "asset_name" in d
        assert "status" in d


class TestEquivalenceSuite:
    def test_run_suite_empty(self):
        suite = run_equivalence_suite([])
        assert suite.total_tested == 0

    def test_run_suite_with_datasets(self):
        assets = [
            Asset(
                id="d1", name="ds1", type=AssetType.DATASET, source_project="P",
                metadata={"schema": [{"name": "id", "type": "int"}]},
                target_fabric_asset={"schema": [{"name": "id", "type": "int"}]},
            ),
            Asset(id="r1", name="r1", type=AssetType.RECIPE_SQL, source_project="P"),
        ]
        suite = run_equivalence_suite(assets)
        assert suite.total_tested == 1  # Only datasets
        assert suite.total_equivalent == 1

    def test_suite_to_dict(self):
        suite = run_equivalence_suite([])
        d = suite.to_dict()
        assert "pass_rate" in d


# ── Regression Suite ──────────────────────────────────────────

class TestRegressionSuite:
    def test_create_baseline(self, tmp_path):
        assets = [
            Asset(id="d1", name="ds1", type=AssetType.DATASET, source_project="P",
                  state=MigrationState.VALIDATED, target_fabric_asset={"table": "t1"}),
        ]
        path = create_baseline(assets, tmp_path / "baseline.json")
        assert path.exists()
        data = json.loads(path.read_text(encoding="utf-8"))
        assert len(data) == 1

    def test_run_regression_no_baseline(self, tmp_path):
        suite = run_regression([], tmp_path / "nonexistent.json")
        assert suite.total_cases == 0

    def test_run_regression_no_changes(self, tmp_path):
        assets = [
            Asset(id="d1", name="ds1", type=AssetType.DATASET, source_project="P",
                  state=MigrationState.VALIDATED, target_fabric_asset={"table": "t1"}),
        ]
        baseline_path = tmp_path / "baseline.json"
        create_baseline(assets, baseline_path)

        suite = run_regression(assets, baseline_path)
        assert suite.total_cases == 1
        assert suite.total_passed == 1
        assert suite.total_failed == 0

    def test_run_regression_state_change(self, tmp_path):
        assets_v1 = [
            Asset(id="d1", name="ds1", type=AssetType.DATASET, source_project="P",
                  state=MigrationState.VALIDATED),
        ]
        baseline_path = tmp_path / "baseline.json"
        create_baseline(assets_v1, baseline_path)

        assets_v2 = [
            Asset(id="d1", name="ds1", type=AssetType.DATASET, source_project="P",
                  state=MigrationState.FAILED, errors=["something broke"]),
        ]
        suite = run_regression(assets_v2, baseline_path)
        assert suite.total_failed > 0

    def test_run_regression_missing_asset(self, tmp_path):
        assets_v1 = [
            Asset(id="d1", name="ds1", type=AssetType.DATASET, source_project="P",
                  state=MigrationState.VALIDATED),
        ]
        baseline_path = tmp_path / "baseline.json"
        create_baseline(assets_v1, baseline_path)

        suite = run_regression([], baseline_path)
        assert suite.total_failed == 1

    def test_suite_to_dict(self, tmp_path):
        assets = [
            Asset(id="d1", name="ds1", type=AssetType.DATASET, source_project="P",
                  state=MigrationState.VALIDATED),
        ]
        baseline_path = tmp_path / "baseline.json"
        create_baseline(assets, baseline_path)
        suite = run_regression(assets, baseline_path)
        d = suite.to_dict()
        assert "pass_rate" in d
        assert "cases" in d

    def test_pass_rate(self):
        suite = RegressionSuite(suite_name="test", total_cases=4, total_passed=3, total_failed=1)
        assert suite.pass_rate == 75.0

    def test_pass_rate_zero(self):
        suite = RegressionSuite(suite_name="empty")
        assert suite.pass_rate == 0.0
