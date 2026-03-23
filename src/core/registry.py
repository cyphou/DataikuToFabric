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

    def save(self, path: Path | None = None) -> None:
        """Save the registry to disk.

        Args:
            path: Override the default path (used for checkpoint files).
        """
        target = path or self.registry_path
        target.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "project_key": self.project_key,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "statistics": self.get_statistics(),
            "assets": [a.model_dump(mode="json") for a in self._assets.values()],
        }
        target.write_text(json.dumps(data, indent=2, default=str))

    def load(self, path: Path | None = None) -> None:
        """Load the registry from disk.

        Args:
            path: Override the default path (used for checkpoint files).
        """
        target = path or self.registry_path
        if not target.exists():
            return
        data = json.loads(target.read_text())
        for item in data.get("assets", []):
            asset = Asset.model_validate(item)
            self._assets[asset.id] = asset

    def save_checkpoint(self, checkpoint_dir: Path, wave_index: int) -> Path:
        """Save a checkpoint file for the given wave.

        Returns the path to the checkpoint file.
        """
        checkpoint_path = checkpoint_dir / f"checkpoint_wave_{wave_index}.json"
        self.save(path=checkpoint_path)
        return checkpoint_path

    def get_completed_agents(self) -> set[str]:
        """Return the set of agent names whose assets are all past DISCOVERED.

        An agent is considered completed if (a) it registered results that
        moved assets beyond DISCOVERED, or (b) there are no assets of that
        agent's type still in DISCOVERED state. This is used by the resume
        logic to decide which agents to skip.
        """
        # Map asset types to responsible agent names
        agent_type_map: dict[str, list[str]] = {
            "discovery": [],  # discovery is done once assets exist
            "sql_converter": [AssetType.RECIPE_SQL.value],
            "python_converter": [AssetType.RECIPE_PYTHON.value],
            "visual_converter": [AssetType.RECIPE_VISUAL.value],
            "dataset_migrator": [AssetType.DATASET.value],
            "connection_mapper": [AssetType.CONNECTION.value],
            "pipeline_builder": [AssetType.FLOW.value, AssetType.SCENARIO.value],
            "validator": [],   # validator is special — runs on everything
        }

        completed: set[str] = set()

        # Discovery is complete if the registry has any assets
        if self._assets:
            completed.add("discovery")

        for agent_name, type_values in agent_type_map.items():
            if agent_name in ("discovery", "validator"):
                continue
            if not type_values:
                continue
            assets_for_agent = [
                a for a in self._assets.values()
                if a.type.value in type_values
            ]
            if not assets_for_agent:
                # No assets of this type → agent has nothing to do, treat as done
                completed.add(agent_name)
                continue
            # Agent is complete if ALL its assets are beyond DISCOVERED
            all_past_discovered = all(
                a.state != MigrationState.DISCOVERED for a in assets_for_agent
            )
            if all_past_discovered:
                completed.add(agent_name)

        return completed

    def reset_assets_for_agent(self, agent_name: str) -> int:
        """Reset all assets belonging to an agent back to DISCOVERED.

        Returns the number of assets reset.
        """
        agent_type_map: dict[str, list[str]] = {
            "sql_converter": [AssetType.RECIPE_SQL.value],
            "python_converter": [AssetType.RECIPE_PYTHON.value],
            "visual_converter": [AssetType.RECIPE_VISUAL.value],
            "dataset_migrator": [AssetType.DATASET.value],
            "connection_mapper": [AssetType.CONNECTION.value],
            "pipeline_builder": [AssetType.FLOW.value, AssetType.SCENARIO.value],
        }
        type_values = agent_type_map.get(agent_name, [])
        count = 0
        for asset in self._assets.values():
            if asset.type.value in type_values and asset.state != MigrationState.DISCOVERED:
                asset.state = MigrationState.DISCOVERED
                asset.errors.clear()
                count += 1
        return count

    def filter_asset_ids(self, asset_ids: set[str]) -> None:
        """Keep only the specified asset IDs, remove everything else."""
        self._assets = {
            aid: a for aid, a in self._assets.items() if aid in asset_ids
        }
