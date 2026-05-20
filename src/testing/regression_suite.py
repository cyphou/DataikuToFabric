"""Regression suite — snapshot-based regression testing for migrations."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.core.registry import AssetRegistry
from src.models.asset import Asset


@dataclass
class RegressionCase:
    """A single regression test case."""
    case_id: str
    asset_name: str
    input_snapshot: dict[str, Any]
    expected_output: dict[str, Any]
    actual_output: dict[str, Any] = field(default_factory=dict)
    passed: bool = False
    diff: list[str] = field(default_factory=list)


@dataclass
class RegressionSuite:
    """Suite of regression tests with baseline and results."""
    suite_name: str
    cases: list[RegressionCase] = field(default_factory=list)
    total_passed: int = 0
    total_failed: int = 0
    total_cases: int = 0

    @property
    def pass_rate(self) -> float:
        return self.total_passed / self.total_cases * 100 if self.total_cases > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "suite_name": self.suite_name,
            "total_cases": self.total_cases,
            "total_passed": self.total_passed,
            "total_failed": self.total_failed,
            "pass_rate": round(self.pass_rate, 1),
            "cases": [
                {
                    "case_id": c.case_id,
                    "asset_name": c.asset_name,
                    "passed": c.passed,
                    "diff": c.diff,
                }
                for c in self.cases
            ],
        }


def create_baseline(
    assets: list[Asset],
    baseline_path: str | Path,
) -> Path:
    """Save a regression baseline from current assets.

    Captures each asset's target_fabric_asset, state, errors, and review flags
    as the expected output for future regression testing.

    Args:
        assets: List of assets to baseline.
        baseline_path: File path for the baseline JSON.

    Returns:
        Path to saved baseline file.
    """
    baseline: list[dict[str, Any]] = []
    for asset in assets:
        baseline.append({
            "asset_id": asset.id,
            "asset_name": asset.name,
            "asset_type": asset.type.value,
            "state": asset.state.value,
            "target_fabric_asset": asset.target_fabric_asset,
            "error_count": len(asset.errors),
            "review_flag_count": len(asset.review_flags),
            "metadata_keys": sorted(asset.metadata.keys()),
        })

    p = Path(baseline_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(baseline, indent=2), encoding="utf-8")
    return p


def run_regression(
    assets: list[Asset],
    baseline_path: str | Path,
    suite_name: str = "regression",
) -> RegressionSuite:
    """Run regression tests by comparing current assets against a saved baseline.

    Args:
        assets: Current list of assets.
        baseline_path: Path to baseline JSON file.
        suite_name: Name for the test suite.

    Returns:
        RegressionSuite with pass/fail results.
    """
    suite = RegressionSuite(suite_name=suite_name)
    p = Path(baseline_path)

    if not p.exists():
        return suite

    baseline: list[dict[str, Any]] = json.loads(p.read_text(encoding="utf-8"))
    baseline_map = {b["asset_id"]: b for b in baseline}
    asset_map = {a.id: a for a in assets}

    for asset_id, expected in baseline_map.items():
        case = RegressionCase(
            case_id=f"reg_{asset_id}",
            asset_name=expected["asset_name"],
            input_snapshot=expected,
            expected_output=expected,
        )

        actual_asset = asset_map.get(asset_id)
        if not actual_asset:
            case.passed = False
            case.diff.append(f"Asset '{expected['asset_name']}' not found in current registry")
            suite.cases.append(case)
            suite.total_failed += 1
            suite.total_cases += 1
            continue

        case.actual_output = {
            "asset_id": actual_asset.id,
            "asset_name": actual_asset.name,
            "asset_type": actual_asset.type.value,
            "state": actual_asset.state.value,
            "target_fabric_asset": actual_asset.target_fabric_asset,
            "error_count": len(actual_asset.errors),
            "review_flag_count": len(actual_asset.review_flags),
            "metadata_keys": sorted(actual_asset.metadata.keys()),
        }

        # Compare
        diffs: list[str] = []
        if expected["state"] != actual_asset.state.value:
            diffs.append(f"State: {expected['state']} → {actual_asset.state.value}")

        if expected.get("error_count", 0) < len(actual_asset.errors):
            diffs.append(
                f"Errors increased: {expected.get('error_count', 0)} → {len(actual_asset.errors)}"
            )

        if expected.get("review_flag_count", 0) < len(actual_asset.review_flags):
            diffs.append(
                f"Review flags increased: {expected.get('review_flag_count', 0)} → {len(actual_asset.review_flags)}"
            )

        # Check target info regression
        if expected.get("target_fabric_asset") and not actual_asset.target_fabric_asset:
            diffs.append("Target Fabric asset info lost")

        case.diff = diffs
        case.passed = len(diffs) == 0
        suite.cases.append(case)

        if case.passed:
            suite.total_passed += 1
        else:
            suite.total_failed += 1
        suite.total_cases += 1

    return suite
