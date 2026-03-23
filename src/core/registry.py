"""Asset Registry — central state store for all migration assets."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.models.asset import Asset, AssetType, MigrationState


class AssetRegistry:
    """In-memory registry backed by a JSON file."""

    def __init__(self, project_key: str, registry_path: Path | None = None):
        self.project_key = project_key
        self.registry_path = registry_path or Path("output") / "registry.json"
        self._assets: dict[str, Asset] = {}

    # ── Write operations ──────────────────────────────────────

    def add_asset(self, asset: Asset) -> None:
        """Add a new asset to the registry."""
        self._assets[asset.id] = asset

    def update_state(self, asset_id: str, new_state: MigrationState) -> None:
        """Transition an asset to a new state."""
        asset = self._assets[asset_id]
        asset.state = new_state

        now = datetime.now(timezone.utc)
        if new_state == MigrationState.CONVERTED:
            asset.timestamps.converted_at = now
        elif new_state == MigrationState.DEPLOYED:
            asset.timestamps.deployed_at = now
        elif new_state == MigrationState.VALIDATED:
            asset.timestamps.validated_at = now

    def set_target(self, asset_id: str, fabric_info: dict) -> None:
        """Set the Fabric target asset info after conversion."""
        self._assets[asset_id].target_fabric_asset = fabric_info

    def add_error(self, asset_id: str, error: str) -> None:
        """Record an error against an asset."""
        self._assets[asset_id].errors.append(error)

    def add_review_flag(self, asset_id: str, flag: str) -> None:
        """Flag an asset for human review."""
        self._assets[asset_id].review_flags.append(flag)

    # ── Query operations ──────────────────────────────────────

    def get_asset(self, asset_id: str) -> Asset | None:
        return self._assets.get(asset_id)

    def get_by_type(self, asset_type: AssetType) -> list[Asset]:
        return [a for a in self._assets.values() if a.type == asset_type]

    def get_by_state(self, state: MigrationState) -> list[Asset]:
        return [a for a in self._assets.values() if a.state == state]

    def get_all(self) -> list[Asset]:
        return list(self._assets.values())

    def get_dependencies(self, asset_id: str) -> list[Asset]:
        """Get all assets that this asset depends on."""
        asset = self._assets.get(asset_id)
        if not asset:
            return []
        return [self._assets[dep] for dep in asset.dependencies if dep in self._assets]

    # ── Statistics ────────────────────────────────────────────

    def get_statistics(self) -> dict:
        all_assets = list(self._assets.values())
        by_state: dict[str, int] = {}
        by_type: dict[str, int] = {}
        for a in all_assets:
            by_state[a.state.value] = by_state.get(a.state.value, 0) + 1
            by_type[a.type.value] = by_type.get(a.type.value, 0) + 1
        return {
            "total": len(all_assets),
            "by_state": by_state,
            "by_type": by_type,
        }

    # ── Persistence ───────────────────────────────────────────

    def save(self) -> None:
        """Save the registry to disk."""
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "project_key": self.project_key,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "statistics": self.get_statistics(),
            "assets": [a.model_dump(mode="json") for a in self._assets.values()],
        }
        self.registry_path.write_text(json.dumps(data, indent=2, default=str))

    def load(self) -> None:
        """Load the registry from disk."""
        if not self.registry_path.exists():
            return
        data = json.loads(self.registry_path.read_text())
        for item in data.get("assets", []):
            asset = Asset.model_validate(item)
            self._assets[asset.id] = asset
