"""Phase 14 — Pre-deploy snapshot and selective rollback.

Provides:
- ``RegistrySnapshot`` — immutable copy of the AssetRegistry state at a point in time.
- ``SnapshotStore`` — persist and reload snapshots from disk.
- ``RollbackPlan`` — describes which assets would be affected by a rollback.
- ``RollbackEngine`` — applies (or simulates) a rollback from a snapshot.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.core.logger import get_logger

logger = get_logger(__name__)


# ── Snapshot model ────────────────────────────────────────────────────────────


class RegistrySnapshot:
    """Immutable copy of the registry state captured before a deployment.

    Attributes:
        snapshot_id: Unique identifier (typically the deployment run_id).
        project_key: Dataiku project key.
        captured_at: ISO-8601 UTC timestamp.
        assets: List of serialised asset dicts at capture time.
    """

    def __init__(
        self,
        snapshot_id: str,
        project_key: str,
        assets: list[dict[str, Any]],
        *,
        captured_at: str | None = None,
    ) -> None:
        self.snapshot_id = snapshot_id
        self.project_key = project_key
        self.captured_at = captured_at or datetime.now(timezone.utc).isoformat()
        self.assets = assets  # list of asset dicts (read-only after creation)

    # ── Serialisation ─────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "project_key": self.project_key,
            "captured_at": self.captured_at,
            "asset_count": len(self.assets),
            "assets": self.assets,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "RegistrySnapshot":
        return cls(
            snapshot_id=d["snapshot_id"],
            project_key=d["project_key"],
            assets=d.get("assets", []),
            captured_at=d.get("captured_at"),
        )

    # ── Lookup helpers ────────────────────────────────────────

    def get_asset(self, asset_id: str) -> dict[str, Any] | None:
        """Return the snapshotted asset dict for the given ID, or None."""
        for asset in self.assets:
            if asset.get("id") == asset_id:
                return asset
        return None

    def assets_by_type(self, asset_type: str) -> list[dict[str, Any]]:
        """Return all snapshotted assets of a given type string."""
        return [a for a in self.assets if a.get("type") == asset_type]


def capture_snapshot(
    snapshot_id: str,
    project_key: str,
    registry: Any,
) -> RegistrySnapshot:
    """Build a ``RegistrySnapshot`` from the current state of an ``AssetRegistry``.

    Args:
        snapshot_id: Identifier for this snapshot (use the deployment run_id).
        project_key: Dataiku project key.
        registry: An ``AssetRegistry`` instance with a ``list_assets()`` method.

    Returns:
        Frozen ``RegistrySnapshot`` reflecting the registry state right now.
    """
    assets = []
    for asset in registry.list_assets():
        try:
            assets.append(asset.model_dump(mode="json"))
        except AttributeError:
            # Fallback for non-Pydantic objects
            assets.append(dict(vars(asset)))

    snapshot = RegistrySnapshot(snapshot_id, project_key, assets)
    logger.info(
        "snapshot_captured",
        snapshot_id=snapshot_id,
        project_key=project_key,
        asset_count=len(assets),
    )
    return snapshot


# ── Snapshot store ────────────────────────────────────────────────────────────


class SnapshotStore:
    """Persist and reload registry snapshots.

    Layout::

        <snapshot_dir>/
            snapshot_<snapshot_id>.json
    """

    def __init__(self, snapshot_dir: Path) -> None:
        self.snapshot_dir = Path(snapshot_dir)
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

    def save(self, snapshot: RegistrySnapshot) -> Path:
        """Write snapshot to disk. Returns the written file path."""
        path = self.snapshot_dir / f"snapshot_{snapshot.snapshot_id}.json"
        path.write_text(json.dumps(snapshot.to_dict(), indent=2, default=str), encoding="utf-8")
        logger.info(
            "snapshot_saved",
            snapshot_id=snapshot.snapshot_id,
            path=str(path),
            assets=len(snapshot.assets),
        )
        return path

    def load(self, snapshot_id: str) -> RegistrySnapshot | None:
        """Load a snapshot by ID. Returns None if not found."""
        path = self.snapshot_dir / f"snapshot_{snapshot_id}.json"
        if not path.exists():
            return None
        return RegistrySnapshot.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def list_snapshots(self) -> list[str]:
        """Return sorted list of snapshot IDs (oldest first)."""
        ids = []
        for p in self.snapshot_dir.glob("snapshot_*.json"):
            ids.append(p.stem.removeprefix("snapshot_"))
        return sorted(ids)

    def delete(self, snapshot_id: str) -> bool:
        """Delete a snapshot. Returns True if deleted, False if not found."""
        path = self.snapshot_dir / f"snapshot_{snapshot_id}.json"
        if path.exists():
            path.unlink()
            return True
        return False


# ── Rollback plan ─────────────────────────────────────────────────────────────


class RollbackPlan:
    """Describes the impact of a rollback without applying it.

    Attributes:
        snapshot_id: The snapshot that would be restored.
        assets_to_restore: Assets whose state would change.
        assets_not_in_snapshot: Assets that exist now but were absent in the snapshot
            (they would be removed if a full rollback were applied).
        asset_type_filter: If set, rollback only applies to this asset type.
        dry_run: If True, nothing has been applied yet.
    """

    def __init__(
        self,
        snapshot_id: str,
        assets_to_restore: list[dict[str, Any]],
        assets_not_in_snapshot: list[str],
        *,
        asset_type_filter: str | None = None,
        dry_run: bool = True,
    ) -> None:
        self.snapshot_id = snapshot_id
        self.assets_to_restore = assets_to_restore
        self.assets_not_in_snapshot = assets_not_in_snapshot
        self.asset_type_filter = asset_type_filter
        self.dry_run = dry_run

    def summary(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "dry_run": self.dry_run,
            "asset_type_filter": self.asset_type_filter,
            "assets_to_restore": len(self.assets_to_restore),
            "assets_not_in_snapshot": len(self.assets_not_in_snapshot),
        }


# ── Rollback engine ───────────────────────────────────────────────────────────


class RollbackEngine:
    """Apply or simulate a rollback from a previously captured snapshot.

    The engine works against an ``AssetRegistry``-compatible object. It uses
    ``registry.register_asset()`` (or ``add_asset()``) and
    ``registry.list_assets()`` for read access.

    For dry-run mode, the registry is never mutated — only a ``RollbackPlan``
    is returned describing what *would* happen.
    """

    def __init__(self, store: SnapshotStore) -> None:
        self.store = store

    # ── Public API ────────────────────────────────────────────

    def plan(
        self,
        snapshot_id: str,
        registry: Any,
        *,
        asset_type_filter: str | None = None,
    ) -> RollbackPlan | None:
        """Build a dry-run plan without mutating the registry.

        Args:
            snapshot_id: The snapshot to plan a rollback from.
            registry: Current ``AssetRegistry`` (read-only access).
            asset_type_filter: Only include assets of this type.

        Returns:
            ``RollbackPlan`` or None if the snapshot cannot be found.
        """
        snapshot = self.store.load(snapshot_id)
        if snapshot is None:
            logger.warning("rollback_snapshot_not_found", snapshot_id=snapshot_id)
            return None

        return self._build_plan(snapshot, registry, asset_type_filter, dry_run=True)

    def apply(
        self,
        snapshot_id: str,
        registry: Any,
        *,
        asset_type_filter: str | None = None,
    ) -> RollbackPlan | None:
        """Apply a rollback from a snapshot to the registry.

        Only assets matching ``asset_type_filter`` (if given) are restored.
        Assets that exist in the registry but not in the snapshot are left
        untouched (safe partial rollback — no deletions).

        Args:
            snapshot_id: The snapshot to restore from.
            registry: ``AssetRegistry`` that supports ``register_asset()``.
            asset_type_filter: Only roll back assets of this type.

        Returns:
            Executed ``RollbackPlan`` (dry_run=False) or None if snapshot missing.
        """
        snapshot = self.store.load(snapshot_id)
        if snapshot is None:
            logger.warning("rollback_snapshot_not_found", snapshot_id=snapshot_id)
            return None

        plan = self._build_plan(snapshot, registry, asset_type_filter, dry_run=False)
        self._apply_plan(plan, snapshot, registry)
        return plan

    # ── Internals ─────────────────────────────────────────────

    def _build_plan(
        self,
        snapshot: RegistrySnapshot,
        registry: Any,
        asset_type_filter: str | None,
        dry_run: bool,
    ) -> RollbackPlan:
        """Build the rollback plan (does not apply anything)."""
        current_ids = {a.id for a in registry.list_assets()}

        candidate_assets = snapshot.assets
        if asset_type_filter:
            candidate_assets = [
                a for a in candidate_assets if a.get("type") == asset_type_filter
            ]

        # Assets in snapshot that differ from (or are absent from) current state
        to_restore = []
        for asset_dict in candidate_assets:
            aid = asset_dict.get("id")
            if not aid:
                continue
            # Always mark for restore to reset state (including state transitions)
            to_restore.append(asset_dict)

        # Assets present now but absent from the (filtered) snapshot
        snapshot_ids = {a.get("id") for a in candidate_assets if a.get("id")}
        not_in_snapshot = [
            aid for aid in current_ids
            if aid not in snapshot_ids and (
                asset_type_filter is None or _get_current_type(registry, aid) == asset_type_filter
            )
        ]

        return RollbackPlan(
            snapshot_id=snapshot.snapshot_id,
            assets_to_restore=to_restore,
            assets_not_in_snapshot=not_in_snapshot,
            asset_type_filter=asset_type_filter,
            dry_run=dry_run,
        )

    def _apply_plan(
        self,
        plan: RollbackPlan,
        snapshot: RegistrySnapshot,
        registry: Any,
    ) -> None:
        """Restore assets from snapshot into the registry."""
        from src.models.asset import Asset

        restored = 0
        for asset_dict in plan.assets_to_restore:
            try:
                asset = Asset.model_validate(asset_dict)
                # Use register_asset if available, otherwise add_asset
                if hasattr(registry, "register_asset"):
                    registry.register_asset(asset)
                elif hasattr(registry, "add_asset"):
                    registry.add_asset(asset)
                restored += 1
            except Exception as exc:
                logger.error(
                    "rollback_asset_restore_failed",
                    asset_id=asset_dict.get("id"),
                    error=str(exc),
                )

        logger.info(
            "rollback_applied",
            snapshot_id=plan.snapshot_id,
            restored=restored,
            skipped_not_in_snapshot=len(plan.assets_not_in_snapshot),
            asset_type_filter=plan.asset_type_filter,
        )


def _get_current_type(registry: Any, asset_id: str) -> str | None:
    """Helper: get type string of an asset in the registry (returns None if missing)."""
    for asset in registry.list_assets():
        if asset.id == asset_id:
            t = asset.type
            return t.value if hasattr(t, "value") else str(t)
    return None
