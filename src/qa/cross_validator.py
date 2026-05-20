"""Cross-validator — checks consistency between related migration artifacts."""

from __future__ import annotations

from src.models.asset import Asset, AssetType, MigrationState


def cross_validate(assets: list[Asset]) -> list[str]:
    """Validate cross-artifact consistency.

    Checks:
    - All referenced dependencies exist in the asset list.
    - Datasets referenced by recipes have targets assigned.
    - Converted recipes have matching target info.

    Returns:
        List of finding strings.
    """
    findings: list[str] = []
    asset_ids = {a.id for a in assets}
    asset_map = {a.id: a for a in assets}

    for asset in assets:
        # Check dependency references
        for dep_id in asset.dependencies:
            if dep_id not in asset_ids:
                findings.append(
                    f"{asset.name}: depends on '{dep_id}' which is not in the registry"
                )

        # Converted recipes should have target info
        if asset.type in (
            AssetType.RECIPE_SQL,
            AssetType.RECIPE_PYTHON,
            AssetType.RECIPE_VISUAL,
        ):
            if asset.state in (MigrationState.CONVERTED, MigrationState.VALIDATED):
                if not asset.target_fabric_asset:
                    findings.append(
                        f"{asset.name}: converted recipe but no target Fabric asset defined"
                    )

        # Datasets in converted state should have schema in target
        if asset.type == AssetType.DATASET:
            if asset.state in (MigrationState.CONVERTED, MigrationState.VALIDATED):
                target = asset.target_fabric_asset or {}
                if not target.get("schema") and not target.get("table_name"):
                    findings.append(
                        f"{asset.name}: converted dataset but target has no schema/table info"
                    )

    return findings
