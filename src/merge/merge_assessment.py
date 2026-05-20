"""Merge assessment — analyze overlap and compatibility across projects."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from src.core.registry import AssetRegistry
from src.models.asset import Asset, AssetType


@dataclass
class AssetFingerprint:
    """Fingerprint for deduplication comparison."""
    asset_id: str
    asset_name: str
    project_key: str
    asset_type: str
    fingerprint: str
    schema_hash: str = ""


@dataclass
class OverlapPair:
    """A pair of potentially duplicate assets."""
    asset_a: AssetFingerprint
    asset_b: AssetFingerprint
    similarity: float
    overlap_type: str  # "exact", "schema_match", "name_match"


@dataclass
class MergeAssessment:
    """Assessment of merge compatibility across multiple projects."""
    project_keys: list[str] = field(default_factory=list)
    total_assets: int = 0
    overlaps: list[OverlapPair] = field(default_factory=list)
    unique_asset_count: int = 0
    overlap_matrix: dict[str, dict[str, int]] = field(default_factory=dict)
    merge_score: float = 100.0  # Higher = easier merge

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_keys": self.project_keys,
            "total_assets": self.total_assets,
            "unique_asset_count": self.unique_asset_count,
            "overlap_count": len(self.overlaps),
            "merge_score": round(self.merge_score, 1),
            "overlap_matrix": self.overlap_matrix,
            "overlaps": [
                {
                    "asset_a": f"{o.asset_a.project_key}/{o.asset_a.asset_name}",
                    "asset_b": f"{o.asset_b.project_key}/{o.asset_b.asset_name}",
                    "similarity": round(o.similarity, 3),
                    "type": o.overlap_type,
                }
                for o in self.overlaps
            ],
        }


def _compute_fingerprint(asset: Asset, project_key: str) -> AssetFingerprint:
    """Compute a fingerprint for deduplication matching."""
    # Build content hash from metadata, schema, and payload
    content = {
        "type": asset.type.value,
        "schema": asset.metadata.get("schema", []),
        "payload": asset.metadata.get("payload", ""),
    }
    raw = json.dumps(content, sort_keys=True, default=str)
    fp = hashlib.sha256(raw.encode()).hexdigest()[:16]

    schema_raw = json.dumps(asset.metadata.get("schema", []), sort_keys=True)
    schema_hash = hashlib.sha256(schema_raw.encode()).hexdigest()[:16]

    return AssetFingerprint(
        asset_id=asset.id,
        asset_name=asset.name,
        project_key=project_key,
        asset_type=asset.type.value,
        fingerprint=fp,
        schema_hash=schema_hash,
    )


def _compute_name_similarity(name_a: str, name_b: str) -> float:
    """Simple name similarity based on common characters."""
    a = name_a.lower()
    b = name_b.lower()
    if a == b:
        return 1.0
    common = set(a) & set(b)
    total = set(a) | set(b)
    return len(common) / len(total) if total else 0.0


def assess_merge(
    registries: list[AssetRegistry],
    similarity_threshold: float = 0.85,
) -> MergeAssessment:
    """Assess merge compatibility across multiple project registries.

    Args:
        registries: List of AssetRegistry instances from different projects.
        similarity_threshold: Minimum similarity for overlap detection.

    Returns:
        MergeAssessment with overlap analysis and merge score.
    """
    result = MergeAssessment()
    all_fingerprints: list[AssetFingerprint] = []

    # Collect fingerprints from all projects
    for registry in registries:
        pk = registry.project_key
        result.project_keys.append(pk)
        for asset in registry.get_all():
            fp = _compute_fingerprint(asset, pk)
            all_fingerprints.append(fp)
            result.total_assets += 1

    # Initialize overlap matrix
    for pk_a in result.project_keys:
        result.overlap_matrix[pk_a] = {}
        for pk_b in result.project_keys:
            result.overlap_matrix[pk_a][pk_b] = 0

    # Compare across projects
    seen_fingerprints: set[str] = set()
    for i, fp_a in enumerate(all_fingerprints):
        seen_fingerprints.add(fp_a.fingerprint)
        for fp_b in all_fingerprints[i + 1:]:
            if fp_a.project_key == fp_b.project_key:
                continue

            # Check exact fingerprint match
            if fp_a.fingerprint == fp_b.fingerprint:
                result.overlaps.append(OverlapPair(
                    asset_a=fp_a, asset_b=fp_b,
                    similarity=1.0, overlap_type="exact",
                ))
                result.overlap_matrix[fp_a.project_key][fp_b.project_key] += 1
                result.overlap_matrix[fp_b.project_key][fp_a.project_key] += 1
                continue

            # Check schema match
            if fp_a.schema_hash and fp_a.schema_hash == fp_b.schema_hash:
                result.overlaps.append(OverlapPair(
                    asset_a=fp_a, asset_b=fp_b,
                    similarity=0.9, overlap_type="schema_match",
                ))
                result.overlap_matrix[fp_a.project_key][fp_b.project_key] += 1
                result.overlap_matrix[fp_b.project_key][fp_a.project_key] += 1
                continue

            # Check name similarity
            if fp_a.asset_type == fp_b.asset_type:
                sim = _compute_name_similarity(fp_a.asset_name, fp_b.asset_name)
                if sim >= similarity_threshold:
                    result.overlaps.append(OverlapPair(
                        asset_a=fp_a, asset_b=fp_b,
                        similarity=sim, overlap_type="name_match",
                    ))

    result.unique_asset_count = len(seen_fingerprints)

    # Merge score: 100 if no overlaps, decreases with overlap ratio
    if result.total_assets > 0:
        overlap_ratio = len(result.overlaps) / result.total_assets
        result.merge_score = max(0.0, 100.0 - overlap_ratio * 100)

    return result
