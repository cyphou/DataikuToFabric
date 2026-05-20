"""Tests for Phase 20 — QA Suite."""

from __future__ import annotations

import pytest

from src.qa.qa_suite import (
    QAResult,
    QAStage,
    StageStatus,
    run_qa_suite,
)
from src.qa.governance import (
    EndorsementLabel,
    GovernanceResult,
    assign_endorsement,
    check_naming_convention,
    detect_pii_columns,
    run_governance,
)
from src.qa.cross_validator import cross_validate
from src.qa.fidelity import FidelityResult, compute_fidelity
from src.qa.comparison_report import generate_qa_html, save_qa_report
from src.models.asset import Asset, AssetType, MigrationState


# ── QA Suite ──────────────────────────────────────────────────

class TestQASuite:
    def test_run_empty(self):
        from src.core.registry import AssetRegistry
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as td:
            reg = AssetRegistry(project_key="T", registry_path=Path(td) / "r.json")
            result = run_qa_suite(reg)
        assert isinstance(result, QAResult)
        assert result.overall_passed is True

    def test_run_with_assets(self, populated_registry):
        result = run_qa_suite(populated_registry)
        assert isinstance(result, QAResult)
        assert len(result.stage_results) > 0

    def test_run_with_fix(self, populated_registry):
        result = run_qa_suite(populated_registry, enable_fix=True)
        assert isinstance(result, QAResult)

    def test_run_with_compare(self, populated_registry):
        result = run_qa_suite(populated_registry, enable_compare=True)
        assert isinstance(result, QAResult)

    def test_result_to_dict(self, populated_registry):
        result = run_qa_suite(populated_registry)
        d = result.to_dict()
        assert "project_key" in d
        assert "stages" in d


# ── Governance ────────────────────────────────────────────────

class TestGovernance:
    def test_detect_pii_none(self):
        asset = Asset(
            id="d1", name="d1", type=AssetType.DATASET, source_project="P",
            metadata={"schema": [{"name": "amount", "type": "int"}]}
        )
        result = detect_pii_columns(asset)
        assert result == []

    def test_detect_pii_ssn(self):
        asset = Asset(
            id="d1", name="d1", type=AssetType.DATASET, source_project="P",
            metadata={"schema": [{"name": "ssn", "type": "varchar"}]}
        )
        result = detect_pii_columns(asset)
        assert len(result) > 0

    def test_detect_pii_email(self):
        asset = Asset(
            id="d1", name="d1", type=AssetType.DATASET, source_project="P",
            metadata={"schema": [{"name": "email", "type": "varchar"}]}
        )
        result = detect_pii_columns(asset)
        assert len(result) > 0

    def test_naming_convention_valid(self):
        asset = Asset(id="ds_orders", name="orders", type=AssetType.DATASET, source_project="P")
        result = check_naming_convention(asset)
        assert isinstance(result, list)

    def test_endorsement_certified(self):
        asset = Asset(
            id="d1", name="d1", type=AssetType.DATASET, source_project="P",
            state=MigrationState.VALIDATED,
            metadata={},
            target_fabric_asset={"table": "t1"},
        )
        label = assign_endorsement(asset)
        assert label == EndorsementLabel.CERTIFIED

    def test_endorsement_failed(self):
        asset = Asset(
            id="d1", name="d1", type=AssetType.DATASET, source_project="P",
            state=MigrationState.FAILED,
            errors=["something broke"],
        )
        label = assign_endorsement(asset)
        assert label in [EndorsementLabel.WARNING, EndorsementLabel.UNENDORSED]

    def test_run_governance(self, populated_registry):
        assets = populated_registry.get_all()
        result = run_governance(assets)
        assert isinstance(result, GovernanceResult)
        assert result.total_checked > 0

    def test_governance_to_dict(self, populated_registry):
        result = run_governance(populated_registry.get_all())
        d = result.to_dict()
        assert "pii_columns" in d


# ── Cross Validator ───────────────────────────────────────────

class TestCrossValidator:
    def test_cross_validate_empty(self):
        findings = cross_validate([])
        assert findings == []

    def test_cross_validate_with_missing_dep(self):
        assets = [
            Asset(
                id="r1", name="r1", type=AssetType.RECIPE_SQL, source_project="P",
                state=MigrationState.CONVERTED,
                dependencies=["nonexistent"],
                target_fabric_asset={"table": "t1"},
            )
        ]
        findings = cross_validate(assets)
        assert len(findings) > 0

    def test_cross_validate_ok(self):
        a1 = Asset(id="d1", name="d1", type=AssetType.DATASET, source_project="P",
                   state=MigrationState.DISCOVERED)
        a2 = Asset(
            id="r1", name="r1", type=AssetType.RECIPE_SQL, source_project="P",
            state=MigrationState.CONVERTED,
            dependencies=["d1"],
            target_fabric_asset={"table": "t1"},
        )
        findings = cross_validate([a1, a2])
        assert isinstance(findings, list)


# ── Fidelity ──────────────────────────────────────────────────

class TestFidelity:
    def test_fidelity_empty(self):
        result = compute_fidelity([])
        assert result.overall_score == 100.0
        assert result.scored_assets == 0

    def test_fidelity_perfect(self):
        assets = [
            Asset(id="d1", name="d1", type=AssetType.DATASET, source_project="P",
                  state=MigrationState.VALIDATED, target_fabric_asset={"table": "t1"})
        ]
        result = compute_fidelity(assets)
        assert result.overall_score == 100.0

    def test_fidelity_with_errors(self):
        assets = [
            Asset(id="d1", name="d1", type=AssetType.DATASET, source_project="P",
                  state=MigrationState.FAILED, errors=["err1", "err2"])
        ]
        result = compute_fidelity(assets)
        assert result.overall_score < 100.0

    def test_fidelity_to_dict(self):
        assets = [
            Asset(id="d1", name="d1", type=AssetType.DATASET, source_project="P",
                  state=MigrationState.VALIDATED)
        ]
        result = compute_fidelity(assets)
        d = result.to_dict()
        assert "overall_score" in d
        assert "penalties" in d


# ── Comparison Report ─────────────────────────────────────────

class TestComparisonReport:
    def test_generate_html(self, populated_registry):
        qa_result = run_qa_suite(populated_registry)
        html = generate_qa_html(qa_result)
        assert "<html" in html

    def test_save_report(self, tmp_path, populated_registry):
        qa_result = run_qa_suite(populated_registry)
        path = save_qa_report(qa_result, tmp_path / "qa.html")
        assert path.exists()
