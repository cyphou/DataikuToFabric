"""Snapshot — captures point-in-time schema/registry state."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.core.registry import AssetRegistry
from src.models.asset import Asset


@dataclass
class ColumnSnapshot:
    """Snapshot of a single column."""
    name: str
    data_type: str
    nullable: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "data_type": self.data_type, "nullable": self.nullable}


@dataclass
class AssetSnapshot:
    """Snapshot of a single asset's schema/state."""
    asset_id: str
    asset_name: str
    asset_type: str
    state: str
    columns: list[ColumnSnapshot] = field(default_factory=list)
    metadata_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "asset_name": self.asset_name,
            "asset_type": self.asset_type,
            "state": self.state,
            "columns": [c.to_dict() for c in self.columns],
            "metadata_hash": self.metadata_hash,
        }


@dataclass
class MigrationSnapshot:
    """Complete point-in-time snapshot of the migration state."""
    snapshot_id: str = ""
    timestamp: str = ""
    project_key: str = ""
    assets: list[AssetSnapshot] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "timestamp": self.timestamp,
            "project_key": self.project_key,
            "asset_count": len(self.assets),
            "assets": [a.to_dict() for a in self.assets],
        }

    def save(self, path: str | Path) -> Path:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")
        return p

    @classmethod
    def load(cls, path: str | Path) -> MigrationSnapshot:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        snap = cls(
            snapshot_id=data["snapshot_id"],
            timestamp=data["timestamp"],
            project_key=data["project_key"],
        )
        for a in data.get("assets", []):
            columns = [ColumnSnapshot(**c) for c in a.get("columns", [])]
            snap.assets.append(AssetSnapshot(
                asset_id=a["asset_id"],
                asset_name=a["asset_name"],
                asset_type=a["asset_type"],
                state=a["state"],
                columns=columns,
                metadata_hash=a.get("metadata_hash", ""),
            ))
        return snap


def _compute_hash(metadata: dict[str, Any]) -> str:
    """Simple deterministic hash of metadata for change detection."""
    import hashlib
    raw = json.dumps(metadata, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _extract_columns(asset: Asset) -> list[ColumnSnapshot]:
    """Extract column info from asset metadata."""
    schema = asset.metadata.get("schema", [])
    columns: list[ColumnSnapshot] = []
    for col in schema:
        if isinstance(col, dict):
            columns.append(ColumnSnapshot(
                name=col.get("name", ""),
                data_type=col.get("type", col.get("data_type", "unknown")),
                nullable=col.get("nullable", True),
            ))
        elif isinstance(col, str):
            columns.append(ColumnSnapshot(name=col, data_type="unknown"))
    return columns


def take_snapshot(registry: AssetRegistry) -> MigrationSnapshot:
    """Capture current registry state as a snapshot.

    Args:
        registry: Populated AssetRegistry.

    Returns:
        MigrationSnapshot with all asset states and schemas.
    """
    now = datetime.now(timezone.utc)
    snap = MigrationSnapshot(
        snapshot_id=f"snap_{now.strftime('%Y%m%d_%H%M%S')}",
        timestamp=now.isoformat(),
        project_key=registry.project_key,
    )

    for asset in registry.get_all():
        columns = _extract_columns(asset)
        snap.assets.append(AssetSnapshot(
            asset_id=asset.id,
            asset_name=asset.name,
            asset_type=asset.type.value,
            state=asset.state.value,
            columns=columns,
            metadata_hash=_compute_hash(asset.metadata),
        ))

    return snap
