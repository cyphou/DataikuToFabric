"""Fidelity scoring — measures how faithfully the migration preserved semantics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.models.asset import Asset, AssetType, MigrationState


@dataclass
class FidelityResult:
    """Migration fidelity score."""
    overall_score: float = 100.0  # 0-100
    per_asset: dict[str, float] = field(default_factory=dict)
    penalties: list[str] = field(default_factory=list)
    total_assets: int = 0
    scored_assets: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall_score": round(self.overall_score, 1),
            "total_assets": self.total_assets,
            "scored_assets": self.scored_assets,
            "penalty_count": len(self.penalties),
            "penalties": self.penalties,
        }


def compute_fidelity(assets: list[Asset]) -> FidelityResult:
    """Score migration fidelity across all assets.

    Fidelity checks per asset:
    - Has target info → +0 penalty
    - Missing target → -20 penalty
    - Has errors → -30 penalty
    - Has review flags → -10 per flag
    - State is FAILED → -50 penalty
    """
    result = FidelityResult(total_assets=len(assets))
    if not assets:
        return result

    total_score = 0.0
    scored = 0

    for asset in assets:
        score = 100.0
        asset_penalties: list[str] = []

        if asset.state == MigrationState.FAILED:
            score -= 50
            asset_penalties.append(f"{asset.name}: FAILED state (-50)")

        if asset.errors:
            penalty = min(30, len(asset.errors) * 10)
            score -= penalty
            asset_penalties.append(f"{asset.name}: {len(asset.errors)} error(s) (-{penalty})")

        if asset.review_flags:
            penalty = min(20, len(asset.review_flags) * 10)
            score -= penalty
            asset_penalties.append(f"{asset.name}: {len(asset.review_flags)} review flag(s) (-{penalty})")

        if asset.state in (MigrationState.CONVERTED, MigrationState.VALIDATED):
            if not asset.target_fabric_asset:
                score -= 20
                asset_penalties.append(f"{asset.name}: missing target info (-20)")

        score = max(0.0, score)
        result.per_asset[asset.id] = score
        result.penalties.extend(asset_penalties)
        total_score += score
        scored += 1

    result.scored_assets = scored
    result.overall_score = total_score / scored if scored > 0 else 0.0
    return result
