"""Tests for AssetRegistry."""

from src.models.asset import Asset, AssetType, MigrationState


class TestAssetRegistry:
    def test_add_and_get(self, registry):
        asset = Asset(id="a1", name="test", type=AssetType.RECIPE_SQL, source_project="P", state=MigrationState.DISCOVERED)
        registry.add_asset(asset)
        assert registry.get_asset("a1") is not None
        assert registry.get_asset("a1").name == "test"

    def test_update_state(self, registry):
        asset = Asset(id="a1", name="test", type=AssetType.DATASET, source_project="P", state=MigrationState.DISCOVERED)
        registry.add_asset(asset)
        registry.update_state("a1", MigrationState.CONVERTED)
        assert registry.get_asset("a1").state == MigrationState.CONVERTED

    def test_get_by_type(self, populated_registry):
        sql_assets = populated_registry.get_by_type(AssetType.RECIPE_SQL)
        assert len(sql_assets) >= 1
        assert all(a.type == AssetType.RECIPE_SQL for a in sql_assets)

    def test_save_and_load(self, registry, tmp_path):
        asset = Asset(id="a1", name="persist", type=AssetType.DATASET, source_project="P", state=MigrationState.DISCOVERED)
        registry.add_asset(asset)
        registry.save()

        from src.core.registry import AssetRegistry

        registry2 = AssetRegistry(project_key="TEST_PROJECT", registry_path=registry.registry_path)
        registry2.load()
        assert registry2.get_asset("a1").name == "persist"

    def test_statistics(self, populated_registry):
        stats = populated_registry.get_statistics()
        assert stats["total"] == 3
