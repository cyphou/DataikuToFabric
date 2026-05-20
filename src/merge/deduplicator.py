"""Deduplicator — merge duplicate assets across projects."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.core.registry import AssetRegistry
from src.merge.merge_assessment import MergeAssessment, OverlapPair
from src.merge.merge_config import ConflictResolution, MergeConfig
from src.models.asset import Asset


@dataclass
class DeduplicationAction:
    """Describes a deduplication action taken."""
    overlap: OverlapPair
    action: str  # "kept", "renamed", "skipped", "manual_review"
    kept_asset: str = ""
    new_name: str = ""


@dataclass
class DeduplicationResult:
    """Result of deduplicating assets across projects."""
    actions: list[DeduplicationAction] = field(default_factory=list)
    merged_assets: list[dict[str, Any]] = field(default_factory=list)
    conflicts_resolved: int = 0
    conflicts_pending: int = 0
    total_input_assets: int = 0
    total_output_assets: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "conflicts_resolved": self.conflicts_resolved,
            "conflicts_pending": self.conflicts_pending,
            "total_input": self.total_input_assets,
            "total_output": self.total_output_assets,
            "dedup_savings": self.total_input_assets - self.total_output_assets,
            "actions": [
                {
                    "asset_a": f"{a.overlap.asset_a.project_key}/{a.overlap.asset_a.asset_name}",
                    "asset_b": f"{a.overlap.asset_b.project_key}/{a.overlap.asset_b.asset_name}",
                    "action": a.action,
                    "kept_asset": a.kept_asset,
                    "new_name": a.new_name,
                }
                for a in self.actions
            ],
        }


def deduplicate(
    registries: list[AssetRegistry],
    assessment: MergeAssessment,
    config: MergeConfig,
) -> DeduplicationResult:
    """Deduplicate overlapping assets based on merge config.

    Args:
        registries: Source registries.
        assessment: Pre-computed merge assessment with overlaps.
        config: Merge configuration (resolution strategy, thresholds).

    Returns:
        DeduplicationResult with actions taken and merged asset list.
    """
    result = DeduplicationResult()

    # Gather all assets
    all_assets: dict[str, tuple[str, Asset]] = {}  # id -> (project_key, asset)
    for registry in registries:
        pk = registry.project_key
        for asset in registry.get_all():
            all_assets[asset.id] = (pk, asset)
            result.total_input_assets += 1

    # Track which asset IDs to skip (duplicates)
    skipped_ids: set[str] = set()

    for overlap in assessment.overlaps:
        if overlap.similarity < config.similarity_threshold:
            continue

        action = _resolve_conflict(overlap, config)
        result.actions.append(action)

        if action.action == "kept":
            # Skip the duplicate
            skipped_ids.add(overlap.asset_b.asset_id)
            result.conflicts_resolved += 1
        elif action.action == "renamed":
            result.conflicts_resolved += 1
        elif action.action == "manual_review":
            result.conflicts_pending += 1

    # Build merged asset list
    for asset_id, (pk, asset) in all_assets.items():
        if asset_id in skipped_ids:
            continue

        entry: dict[str, Any] = {
            "id": asset.id,
            "name": asset.name,
            "type": asset.type.value,
            "source_project": pk,
        }

        if config.prefix_with_project:
            entry["merged_name"] = f"{pk}__{asset.name}"
        else:
            entry["merged_name"] = asset.name

        result.merged_assets.append(entry)

    result.total_output_assets = len(result.merged_assets)
    return result


def _resolve_conflict(overlap: OverlapPair, config: MergeConfig) -> DeduplicationAction:
    """Resolve a single overlap conflict."""
    if config.conflict_resolution == ConflictResolution.KEEP_FIRST:
        return DeduplicationAction(
            overlap=overlap,
            action="kept",
            kept_asset=f"{overlap.asset_a.project_key}/{overlap.asset_a.asset_name}",
        )
    elif config.conflict_resolution == ConflictResolution.KEEP_LATEST:
        return DeduplicationAction(
            overlap=overlap,
            action="kept",
            kept_asset=f"{overlap.asset_b.project_key}/{overlap.asset_b.asset_name}",
        )
    elif config.conflict_resolution == ConflictResolution.RENAME:
        new_name = f"{overlap.asset_b.project_key}__{overlap.asset_b.asset_name}"
        return DeduplicationAction(
            overlap=overlap,
            action="renamed",
            kept_asset=f"{overlap.asset_a.project_key}/{overlap.asset_a.asset_name}",
            new_name=new_name,
        )
    else:  # MANUAL
        return DeduplicationAction(
            overlap=overlap,
            action="manual_review",
        )
