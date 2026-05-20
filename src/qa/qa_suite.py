"""QA suite orchestrator — validate → auto-fix → governance → compare → report."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from src.core.registry import AssetRegistry
from src.models.asset import Asset, MigrationState
from src.qa.cross_validator import cross_validate
from src.qa.fidelity import compute_fidelity, FidelityResult
from src.qa.governance import run_governance, GovernanceResult


class QAStage(str, Enum):
    VALIDATE = "validate"
    AUTO_FIX = "auto_fix"
    GOVERNANCE = "governance"
    COMPARE = "compare"
    REPORT = "report"


class StageStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    WARNING = "warning"


@dataclass
class StageResult:
    """Result from a single QA stage."""
    stage: QAStage
    status: StageStatus
    findings: list[str] = field(default_factory=list)
    fixes_applied: int = 0
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class QAResult:
    """Full QA suite result."""
    project_key: str
    stage_results: list[StageResult] = field(default_factory=list)
    overall_passed: bool = True
    fidelity: FidelityResult | None = None
    governance: GovernanceResult | None = None
    total_findings: int = 0
    total_fixes: int = 0

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "project_key": self.project_key,
            "overall_passed": self.overall_passed,
            "total_findings": self.total_findings,
            "total_fixes": self.total_fixes,
            "stages": [
                {
                    "stage": sr.stage.value,
                    "status": sr.status.value,
                    "findings_count": len(sr.findings),
                    "fixes_applied": sr.fixes_applied,
                    "findings": sr.findings,
                }
                for sr in self.stage_results
            ],
        }
        if self.fidelity:
            result["fidelity"] = self.fidelity.to_dict()
        if self.governance:
            result["governance"] = self.governance.to_dict()
        return result


def run_qa_suite(
    registry: AssetRegistry,
    *,
    enable_fix: bool = False,
    enable_compare: bool = True,
    healers: Any | None = None,
) -> QAResult:
    """Run the full QA pipeline: validate → fix → governance → compare → report.

    Args:
        registry: Populated registry with converted assets.
        enable_fix: If True, run auto-fix stage (requires healers).
        enable_compare: If True, run comparison stage.
        healers: Optional healer registry for auto-fix stage.

    Returns:
        QAResult with per-stage results and overall status.
    """
    result = QAResult(project_key=registry.project_key)
    all_assets = registry.get_all()

    # Stage 1: Validate
    validate_result = _run_validate_stage(all_assets)
    result.stage_results.append(validate_result)
    result.total_findings += len(validate_result.findings)

    # Stage 2: Auto-fix (optional)
    if enable_fix and healers:
        fix_result = _run_fix_stage(all_assets, healers)
        result.stage_results.append(fix_result)
        result.total_fixes += fix_result.fixes_applied
    else:
        result.stage_results.append(StageResult(
            stage=QAStage.AUTO_FIX,
            status=StageStatus.SKIPPED,
            findings=["Auto-fix disabled or no healers provided"],
        ))

    # Stage 3: Governance
    gov_result_data = run_governance(all_assets)
    result.governance = gov_result_data
    gov_stage = StageResult(
        stage=QAStage.GOVERNANCE,
        status=StageStatus.PASSED if not gov_result_data.pii_columns else StageStatus.WARNING,
        findings=[f"PII detected: {col}" for col in gov_result_data.pii_columns],
        details={"endorsements": gov_result_data.endorsement_summary},
    )
    result.stage_results.append(gov_stage)
    result.total_findings += len(gov_stage.findings)

    # Stage 4: Compare
    if enable_compare:
        compare_result = _run_compare_stage(all_assets)
        result.stage_results.append(compare_result)
        result.total_findings += len(compare_result.findings)
    else:
        result.stage_results.append(StageResult(
            stage=QAStage.COMPARE,
            status=StageStatus.SKIPPED,
            findings=["Comparison disabled"],
        ))

    # Stage 5: Fidelity report
    fidelity = compute_fidelity(all_assets)
    result.fidelity = fidelity
    fidelity_stage = StageResult(
        stage=QAStage.REPORT,
        status=StageStatus.PASSED if fidelity.overall_score >= 80 else StageStatus.WARNING,
        findings=[f"Overall fidelity: {fidelity.overall_score:.1f}%"],
        details={"fidelity_score": fidelity.overall_score},
    )
    result.stage_results.append(fidelity_stage)

    # Overall result
    result.overall_passed = all(
        sr.status in (StageStatus.PASSED, StageStatus.SKIPPED, StageStatus.WARNING)
        for sr in result.stage_results
    )

    return result


def _run_validate_stage(assets: list[Asset]) -> StageResult:
    """Run validation checks on all converted assets."""
    findings: list[str] = []

    for asset in assets:
        # Check state consistency
        if asset.state == MigrationState.FAILED:
            findings.append(f"{asset.name}: in FAILED state")
        if asset.errors:
            findings.append(f"{asset.name}: has {len(asset.errors)} error(s)")
        if asset.review_flags:
            findings.append(f"{asset.name}: has {len(asset.review_flags)} review flag(s)")

        # Check target info present for converted assets
        if asset.state in (MigrationState.CONVERTED, MigrationState.VALIDATED):
            if not asset.target_fabric_asset:
                findings.append(f"{asset.name}: converted but missing target info")

    status = StageStatus.PASSED if not findings else StageStatus.WARNING
    return StageResult(stage=QAStage.VALIDATE, status=status, findings=findings)


def _run_fix_stage(assets: list[Asset], healers: Any) -> StageResult:
    """Run auto-fix healers on all assets."""
    findings: list[str] = []
    fixes = 0

    if hasattr(healers, "heal_all"):
        heal_results = healers.heal_all(assets)
        for hr in heal_results:
            fixes += 1
            findings.append(f"Fixed: {hr}")
    else:
        findings.append("Healer registry has no heal_all method")

    return StageResult(
        stage=QAStage.AUTO_FIX,
        status=StageStatus.PASSED if fixes > 0 else StageStatus.SKIPPED,
        findings=findings,
        fixes_applied=fixes,
    )


def _run_compare_stage(assets: list[Asset]) -> StageResult:
    """Run cross-artifact comparison."""
    findings = cross_validate(assets)
    return StageResult(
        stage=QAStage.COMPARE,
        status=StageStatus.PASSED if not findings else StageStatus.WARNING,
        findings=findings,
    )
