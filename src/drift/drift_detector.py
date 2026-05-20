"""Drift detector — compare two snapshots and identify changes."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from src.drift.snapshot import MigrationSnapshot, AssetSnapshot, ColumnSnapshot


class DriftSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    BREAKING = "breaking"


class DriftType(str, Enum):
    ASSET_ADDED = "asset_added"
    ASSET_REMOVED = "asset_removed"
    STATE_CHANGED = "state_changed"
    COLUMN_ADDED = "column_added"
    COLUMN_REMOVED = "column_removed"
    COLUMN_TYPE_CHANGED = "column_type_changed"
    COLUMN_NULLABLE_CHANGED = "column_nullable_changed"
    METADATA_CHANGED = "metadata_changed"


@dataclass
class DriftItem:
    """A single detected drift."""
    drift_type: DriftType
    severity: DriftSeverity
    asset_name: str
    description: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class DriftReport:
    """Full drift detection report."""
    baseline_id: str
    current_id: str
    project_key: str
    drifts: list[DriftItem] = field(default_factory=list)
    total_assets_baseline: int = 0
    total_assets_current: int = 0

    @property
    def has_breaking_changes(self) -> bool:
        return any(d.severity == DriftSeverity.BREAKING for d in self.drifts)

    @property
    def drift_count(self) -> int:
        return len(self.drifts)

    def to_dict(self) -> dict[str, Any]:
        return {
            "baseline_id": self.baseline_id,
            "current_id": self.current_id,
            "project_key": self.project_key,
            "total_assets_baseline": self.total_assets_baseline,
            "total_assets_current": self.total_assets_current,
            "drift_count": self.drift_count,
            "has_breaking_changes": self.has_breaking_changes,
            "drifts": [
                {
                    "type": d.drift_type.value,
                    "severity": d.severity.value,
                    "asset": d.asset_name,
                    "description": d.description,
                    "details": d.details,
                }
                for d in self.drifts
            ],
        }

    def get_by_severity(self, severity: DriftSeverity) -> list[DriftItem]:
        return [d for d in self.drifts if d.severity == severity]


def detect_drift(baseline: MigrationSnapshot, current: MigrationSnapshot) -> DriftReport:
    """Compare two snapshots and produce a drift report.

    Args:
        baseline: Earlier snapshot (reference).
        current: Later snapshot (new state).

    Returns:
        DriftReport listing all changes.
    """
    report = DriftReport(
        baseline_id=baseline.snapshot_id,
        current_id=current.snapshot_id,
        project_key=current.project_key,
        total_assets_baseline=len(baseline.assets),
        total_assets_current=len(current.assets),
    )

    base_map = {a.asset_id: a for a in baseline.assets}
    curr_map = {a.asset_id: a for a in current.assets}

    # Assets added
    for aid, asset in curr_map.items():
        if aid not in base_map:
            report.drifts.append(DriftItem(
                drift_type=DriftType.ASSET_ADDED,
                severity=DriftSeverity.INFO,
                asset_name=asset.asset_name,
                description=f"New asset added: {asset.asset_name} ({asset.asset_type})",
            ))

    # Assets removed
    for aid, asset in base_map.items():
        if aid not in curr_map:
            report.drifts.append(DriftItem(
                drift_type=DriftType.ASSET_REMOVED,
                severity=DriftSeverity.WARNING,
                asset_name=asset.asset_name,
                description=f"Asset removed: {asset.asset_name} ({asset.asset_type})",
            ))

    # Changed assets
    for aid in base_map:
        if aid not in curr_map:
            continue
        base_asset = base_map[aid]
        curr_asset = curr_map[aid]

        # State change
        if base_asset.state != curr_asset.state:
            report.drifts.append(DriftItem(
                drift_type=DriftType.STATE_CHANGED,
                severity=DriftSeverity.INFO,
                asset_name=curr_asset.asset_name,
                description=f"State changed: {base_asset.state} → {curr_asset.state}",
                details={"old": base_asset.state, "new": curr_asset.state},
            ))

        # Metadata change
        if base_asset.metadata_hash != curr_asset.metadata_hash:
            report.drifts.append(DriftItem(
                drift_type=DriftType.METADATA_CHANGED,
                severity=DriftSeverity.INFO,
                asset_name=curr_asset.asset_name,
                description=f"Metadata changed for {curr_asset.asset_name}",
            ))

        # Column comparison
        _compare_columns(base_asset, curr_asset, report)

    return report


def _compare_columns(
    base: AssetSnapshot,
    current: AssetSnapshot,
    report: DriftReport,
) -> None:
    """Compare columns between two asset snapshots."""
    base_cols = {c.name: c for c in base.columns}
    curr_cols = {c.name: c for c in current.columns}

    for col_name, col in curr_cols.items():
        if col_name not in base_cols:
            report.drifts.append(DriftItem(
                drift_type=DriftType.COLUMN_ADDED,
                severity=DriftSeverity.INFO,
                asset_name=current.asset_name,
                description=f"Column added: {col_name} ({col.data_type})",
            ))

    for col_name, col in base_cols.items():
        if col_name not in curr_cols:
            report.drifts.append(DriftItem(
                drift_type=DriftType.COLUMN_REMOVED,
                severity=DriftSeverity.BREAKING,
                asset_name=current.asset_name,
                description=f"Column removed: {col_name}",
            ))
            continue

        curr_col = curr_cols[col_name]
        if col.data_type != curr_col.data_type:
            report.drifts.append(DriftItem(
                drift_type=DriftType.COLUMN_TYPE_CHANGED,
                severity=DriftSeverity.BREAKING,
                asset_name=current.asset_name,
                description=f"Column '{col_name}' type changed: {col.data_type} → {curr_col.data_type}",
                details={"old": col.data_type, "new": curr_col.data_type},
            ))

        if col.nullable != curr_col.nullable:
            report.drifts.append(DriftItem(
                drift_type=DriftType.COLUMN_NULLABLE_CHANGED,
                severity=DriftSeverity.WARNING,
                asset_name=current.asset_name,
                description=f"Column '{col_name}' nullable changed: {col.nullable} → {curr_col.nullable}",
            ))
