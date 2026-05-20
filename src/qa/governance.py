"""Governance checks — endorsement labels, PII detection, naming conventions."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from src.models.asset import Asset, AssetType

_PII_PATTERNS = [
    (r"\bssn\b", "SSN"),
    (r"\bsocial.?security\b", "Social Security"),
    (r"\bemail\b", "Email"),
    (r"\bphone\b", "Phone"),
    (r"\baddress\b", "Address"),
    (r"\bdate.?of.?birth\b", "Date of Birth"),
    (r"\bdob\b", "Date of Birth"),
    (r"\bcredit.?card\b", "Credit Card"),
    (r"\bpassport\b", "Passport"),
    (r"\bdriver.?licen[cs]e\b", "Driver License"),
    (r"\bnational.?id\b", "National ID"),
    (r"\btax.?id\b", "Tax ID"),
    (r"\biban\b", "IBAN"),
]


class EndorsementLabel(str):
    """Endorsement labels for dataset quality."""
    CERTIFIED = "certified"
    PROMOTED = "promoted"
    WARNING = "warning"
    UNENDORSED = "unendorsed"


@dataclass
class GovernanceResult:
    """Governance check results."""
    pii_columns: list[str] = field(default_factory=list)
    naming_violations: list[str] = field(default_factory=list)
    endorsement_summary: dict[str, int] = field(default_factory=dict)
    total_checked: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "pii_columns": self.pii_columns,
            "naming_violations": self.naming_violations,
            "endorsement_summary": self.endorsement_summary,
            "total_checked": self.total_checked,
        }


def detect_pii_columns(asset: Asset) -> list[str]:
    """Detect potential PII in dataset column names."""
    hits: list[str] = []
    schema = asset.metadata.get("schema", [])
    for col in schema:
        col_name = col.get("name", "") if isinstance(col, dict) else str(col)
        for pattern, label in _PII_PATTERNS:
            if re.search(pattern, col_name, re.IGNORECASE):
                hits.append(f"{asset.name}.{col_name} ({label})")
                break
    return hits


def check_naming_convention(asset: Asset) -> list[str]:
    """Check if asset names follow recommended conventions."""
    violations: list[str] = []
    name = asset.name

    if " " in name:
        violations.append(f"{name}: contains spaces — use underscores")
    if name != name.lower() and "_" not in name:
        violations.append(f"{name}: mixed case without underscores — use snake_case")
    if re.search(r"[^a-zA-Z0-9_\-.]", name):
        violations.append(f"{name}: contains special characters")

    return violations


def assign_endorsement(asset: Asset) -> str:
    """Assign an endorsement label based on asset quality signals."""
    if asset.errors:
        return EndorsementLabel.WARNING
    if asset.review_flags:
        return EndorsementLabel.PROMOTED
    if asset.target_fabric_asset and not asset.errors and not asset.review_flags:
        return EndorsementLabel.CERTIFIED
    return EndorsementLabel.UNENDORSED


def run_governance(assets: list[Asset]) -> GovernanceResult:
    """Run governance checks on all assets."""
    result = GovernanceResult(total_checked=len(assets))
    endorsement_counts: dict[str, int] = {}

    for asset in assets:
        # PII detection on datasets
        if asset.type == AssetType.DATASET:
            pii = detect_pii_columns(asset)
            result.pii_columns.extend(pii)

        # Naming conventions
        violations = check_naming_convention(asset)
        result.naming_violations.extend(violations)

        # Endorsement
        label = assign_endorsement(asset)
        endorsement_counts[label] = endorsement_counts.get(label, 0) + 1

    result.endorsement_summary = endorsement_counts
    return result
