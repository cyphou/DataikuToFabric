"""Tests for Phase 11 â€” Data Migration pipeline."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.agents.dataset_agent import (
    LARGE_FILE_THRESHOLD,
    export_dataset,
    get_watermark,
    load_into_table,
    run_data_migration,
    update_watermark,
    upload_dataset,
    verify_row_counts,
)
from src.models.asset import Asset, AssetType, MigrationState


# â”€â”€ Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


@dataclass
class StubDataikuConfig:
    url: str = "https://fake.dataiku.com"
    api_key_env: str = "DATAIKU_API_KEY"
    project_key: str = "PROJ"


@dataclass
class StubFabricConfig:
    workspace_id: str = "ws-0001"
    lakehouse_id: str = "lh-0001"
    lakehouse_name: str = "lh_test"
    warehouse_id: str = "wh-0001"
    warehouse_name: str = "wh_test"


@dataclass
class StubMigrationConfig:
    output_dir: str = "./output"
    default_storage: str = "lakehouse"
    target_sql_dialect: str = "tsql"
    export_format: str = "parquet"
    chunk_size_mb: int = 4
    compression: str = "snappy"
    upload_method: str = "httpx"
    load_mode: str = "overwrite"


@dataclass
class StubConfig:
    dataiku: Any = None
    fabric: Any = None
    migration: Any = None

    def __post_init__(self):
        self.dataiku = self.dataiku or StubDataikuConfig()
        self.fabric = self.fabric or StubFabricConfig()
        self.migration = self.migration or StubMigrationConfig()


@dataclass
class StubContext:
    config: Any = None
    connectors: dict = field(default_factory=dict)


def _make_asset(
    name: str = "orders",
    metadata: dict | None = None,
    target: dict | None = None,
) -> Asset:
    return Asset(
        id=f"ds_{name}",
        type=AssetType.DATASET,
        name=name,
        source_project="PROJ",
        state=MigrationState.DISCOVERED,
        metadata=metadata or {},
        target_fabric_asset=target,
    )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# 1. Export dataset
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

class TestExportDataset:
    """Test export_dataset()."""

    def test_export_parquet(self, tmp_path: Path):
        client = AsyncMock()
        client.export_dataset_to_file.return_value = {
            "path": str(tmp_path / "orders.parquet"),
            "bytes": 1024,
            "incremental": False,
        }
        result = asyncio.run(
            export_dataset(client, "PROJ", "orders", tmp_path, fmt="parquet")
        )
        assert result["path"].endswith("orders.parquet")
        client.export_dataset_to_file.assert_called_once_with(
            "PROJ", "orders", str(tmp_path / "orders.parquet"), fmt="parquet",
            filter_column=None, filter_value=None,
        )

    def test_export_csv(self, tmp_path: Path):
        client = AsyncMock()
        client.export_dataset_to_file.return_value = {
            "path": str(tmp_path / "orders.csv"),
            "bytes": 512,
            "incremental": False,
        }
        result = asyncio.run(
            export_dataset(client, "PROJ", "orders", tmp_path, fmt="csv")
        )
        assert result["path"].endswith("orders.csv")

    def test_export_incremental(self, tmp_path: Path):
        client = AsyncMock()
        client.export_dataset_to_file.return_value = {
            "path": str(tmp_path / "orders.parquet"),
            "bytes": 256,
            "incremental": True,
        }
        result = asyncio.run(
            export_dataset(
                client, "PROJ", "orders", tmp_path,
                incremental_column="updated_at",
                watermark_value="2024-01-01",
            )
        )
        assert result["incremental"] is True
        client.export_dataset_to_file.assert_called_once_with(
            "PROJ", "orders", str(tmp_path / "orders.parquet"), fmt="parquet",
            filter_column="updated_at", filter_value="2024-01-01",
        )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# 2. Upload dataset
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

class TestUploadDataset:
    """Test upload_dataset()."""

    def test_httpx_upload_small_file(self, tmp_path: Path):
        # Create a small file
        data_file = tmp_path / "small.parquet"
        data_file.write_bytes(b"x" * 1024)

        client = AsyncMock()
        client.upload_to_lakehouse.return_value = {"status": "ok", "chunks": 1}

        result = asyncio.run(
            upload_dataset(client, "lh-1", str(data_file), "Files/small.parquet")
        )
        assert result["status"] == "ok"
        client.upload_to_lakehouse.assert_called_once()

    def test_azcopy_upload_when_method_azcopy(self, tmp_path: Path):
        data_file = tmp_path / "big.parquet"
        data_file.write_bytes(b"x" * 1024)

        client = AsyncMock()
        client.workspace_id = "ws-1"
        client.upload_via_azcopy.return_value = {"status": "ok", "method": "azcopy"}

        result = asyncio.run(
            upload_dataset(
                client, "lh-1", str(data_file), "Files/big.parquet",
                upload_method="azcopy",
            )
        )
        assert result["method"] == "azcopy"
        client.upload_via_azcopy.assert_called_once()

    def test_azcopy_fallback_to_httpx(self, tmp_path: Path):
        data_file = tmp_path / "big.parquet"
        data_file.write_bytes(b"x" * 1024)

        client = AsyncMock()
        client.workspace_id = "ws-1"
        client.upload_via_azcopy.side_effect = RuntimeError("azcopy not found")
        client.upload_to_lakehouse.return_value = {"status": "ok", "chunks": 1}

        result = asyncio.run(
            upload_dataset(
                client, "lh-1", str(data_file), "Files/big.parquet",
                upload_method="azcopy",
            )
        )
        assert result["status"] == "ok"
        client.upload_to_lakehouse.assert_called_once()

    def test_chunked_upload_passes_chunk_size(self, tmp_path: Path):
        data_file = tmp_path / "data.parquet"
        data_file.write_bytes(b"x" * 2048)

        client = AsyncMock()
        client.upload_to_lakehouse.return_value = {"status": "ok", "chunks": 1}

        asyncio.run(
            upload_dataset(
                client, "lh-1", str(data_file), "Files/data.parquet",
                chunk_size_mb=8,
            )
        )
        call_kwargs = client.upload_to_lakehouse.call_args
        assert call_kwargs.kwargs.get("chunk_size_mb") == 8 or call_kwargs[1].get("chunk_size_mb") == 8

    def test_progress_callback_forwarded(self, tmp_path: Path):
        data_file = tmp_path / "data.parquet"
        data_file.write_bytes(b"x" * 100)

        callback = MagicMock()
        client = AsyncMock()
        client.upload_to_lakehouse.return_value = {"status": "ok", "chunks": 1}

        asyncio.run(
            upload_dataset(
                client, "lh-1", str(data_file), "Files/data.parquet",
                on_progress=callback,
            )
        )
        call_kwargs = client.upload_to_lakehouse.call_args
        assert call_kwargs.kwargs.get("on_progress") is callback or call_kwargs[1].get("on_progress") is callback


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# 3. Load into table
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

class TestLoadIntoTable:
    """Test load_into_table()."""

    def test_load_overwrite(self):
        client = AsyncMock()
        client.load_table.return_value = {"status": "ok"}

        result = asyncio.run(
            load_into_table(client, "lh-1", "orders", "Files/orders.parquet")
        )
        assert result["status"] == "ok"
        client.load_table.assert_called_once_with(
            "lh-1", "orders", "Files/orders.parquet",
            file_format="parquet", mode="overwrite",
        )

    def test_load_append(self):
        client = AsyncMock()
        client.load_table.return_value = {"status": "ok"}

        result = asyncio.run(
            load_into_table(
                client, "lh-1", "orders", "Files/orders.parquet",
                mode="append",
            )
        )
        client.load_table.assert_called_once_with(
            "lh-1", "orders", "Files/orders.parquet",
            file_format="parquet", mode="append",
        )

    def test_load_csv_format(self):
        client = AsyncMock()
        client.load_table.return_value = {"status": "ok"}

        asyncio.run(
            load_into_table(
                client, "lh-1", "orders", "Files/orders.csv",
                file_format="csv",
            )
        )
        client.load_table.assert_called_once_with(
            "lh-1", "orders", "Files/orders.csv",
            file_format="csv", mode="overwrite",
        )


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# 4. Verify row counts
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

class TestVerifyRowCounts:
    """Test verify_row_counts()."""

    def test_counts_match(self):
        dataiku = AsyncMock()
        fabric = AsyncMock()
        dataiku.get_dataset_row_count.return_value = 1000
        fabric.query_row_count.return_value = 1000

        result = asyncio.run(
            verify_row_counts(
                dataiku, fabric, "PROJ", "orders",
                warehouse_id="wh-1", table_name="orders",
            )
        )
        assert result["match"] is True
        assert result["source_count"] == 1000
        assert result["target_count"] == 1000

    def test_counts_mismatch(self):
        dataiku = AsyncMock()
        fabric = AsyncMock()
        dataiku.get_dataset_row_count.return_value = 1000
        fabric.query_row_count.return_value = 999

        result = asyncio.run(
            verify_row_counts(
                dataiku, fabric, "PROJ", "orders",
                warehouse_id="wh-1", table_name="orders",
            )
        )
        assert result["match"] is False

    def test_source_unavailable(self):
        dataiku = AsyncMock()
        fabric = AsyncMock()
        dataiku.get_dataset_row_count.return_value = None
        fabric.query_row_count.return_value = 1000

        result = asyncio.run(
            verify_row_counts(
                dataiku, fabric, "PROJ", "orders",
                warehouse_id="wh-1", table_name="orders",
            )
        )
        assert result["match"] is False
        assert result["source_available"] is False

    def test_no_warehouse_skips_target_query(self):
        dataiku = AsyncMock()
        fabric = AsyncMock()
        dataiku.get_dataset_row_count.return_value = 500

        result = asyncio.run(
            verify_row_counts(dataiku, fabric, "PROJ", "orders")
        )
        assert result["target_count"] is None
        assert result["target_available"] is False
        fabric.query_row_count.assert_not_called()


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# 5. Watermark helpers
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

class TestWatermark:
    """Test get_watermark() / update_watermark()."""

    def test_get_watermark_present(self):
        meta = {"incremental": {"column": "updated_at", "last_watermark": "2024-06-01"}}
        col, val = get_watermark(meta)
        assert col == "updated_at"
        assert val == "2024-06-01"

    def test_get_watermark_missing(self):
        col, val = get_watermark({})
        assert col is None
        assert val is None

    def test_get_watermark_partial(self):
        meta = {"incremental": {"column": "id"}}
        col, val = get_watermark(meta)
        assert col == "id"
        assert val is None

    def test_update_watermark_new(self):
        meta: dict = {}
        update_watermark(meta, "id", "42")
        assert meta["incremental"]["column"] == "id"
        assert meta["incremental"]["last_watermark"] == "42"

    def test_update_watermark_existing(self):
        meta = {"incremental": {"column": "id", "last_watermark": "10"}}
        update_watermark(meta, "id", "50")
        assert meta["incremental"]["last_watermark"] == "50"


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# 6. Full data migration pipeline
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

class TestRunDataMigration:
    """Test run_data_migration() end-to-end."""

    def _make_context(
        self,
        dataiku_client=None,
        fabric_client=None,
        config=None,
    ) -> StubContext:
        ctx = StubContext(
            config=config or StubConfig(),
            connectors={
                "dataiku": dataiku_client,
                "fabric": fabric_client,
            },
        )
        return ctx

    def test_skipped_when_no_connectors(self, tmp_path: Path):
        ctx = StubContext(config=StubConfig(), connectors={})
        asset = _make_asset()
        result = asyncio.run(
            run_data_migration(ctx, asset, tmp_path)
        )
        assert result["status"] == "skipped"

    def test_full_pipeline_lakehouse(self, tmp_path: Path):
        dataiku = AsyncMock()
        fabric = AsyncMock()

        # Step 1: export
        export_file = tmp_path / "orders.parquet"
        export_file.write_bytes(b"fake-parquet-data")
        dataiku.export_dataset_to_file.return_value = {
            "path": str(export_file),
            "bytes": 17,
            "incremental": False,
        }

        # Step 2: upload
        fabric.upload_to_lakehouse.return_value = {"status": "ok", "chunks": 1}

        # Step 3: load
        fabric.load_table.return_value = {"status": "ok"}

        # Step 4: verify
        dataiku.get_dataset_row_count.return_value = 100
        fabric.query_row_count.return_value = None  # No warehouse for lakehouse path

        asset = _make_asset(target={"storage": "lakehouse"})
        ctx = self._make_context(dataiku_client=dataiku, fabric_client=fabric)

        result = asyncio.run(
            run_data_migration(ctx, asset, tmp_path)
        )

        assert result["status"] == "completed"
        assert "export" in result
        assert "upload" in result
        assert "load" in result
        assert "row_count_verification" in result
        dataiku.export_dataset_to_file.assert_called_once()
        fabric.upload_to_lakehouse.assert_called_once()
        fabric.load_table.assert_called_once()

    def test_full_pipeline_warehouse(self, tmp_path: Path):
        dataiku = AsyncMock()
        fabric = AsyncMock()

        export_file = tmp_path / "sales.parquet"
        export_file.write_bytes(b"fake-parquet-data")
        dataiku.export_dataset_to_file.return_value = {
            "path": str(export_file),
            "bytes": 17,
            "incremental": False,
        }
        fabric.upload_to_lakehouse.return_value = {"status": "ok", "chunks": 1}
        dataiku.get_dataset_row_count.return_value = 200
        fabric.query_row_count.return_value = 200

        asset = _make_asset(name="sales", target={"storage": "warehouse"})
        cfg = StubConfig()
        cfg.fabric.warehouse_id = "wh-0001"
        ctx = self._make_context(dataiku_client=dataiku, fabric_client=fabric, config=cfg)

        result = asyncio.run(
            run_data_migration(ctx, asset, tmp_path)
        )

        assert result["status"] == "completed"
        # Warehouse storage should skip load_table
        assert result["load"] == {}
        # Row count should be verified
        assert result["row_count_verification"]["match"] is True

    def test_incremental_watermark(self, tmp_path: Path):
        dataiku = AsyncMock()
        fabric = AsyncMock()

        export_file = tmp_path / "events.parquet"
        export_file.write_bytes(b"fake-data")
        dataiku.export_dataset_to_file.return_value = {
            "path": str(export_file),
            "bytes": 9,
            "incremental": True,
        }
        fabric.upload_to_lakehouse.return_value = {"status": "ok", "chunks": 1}
        fabric.load_table.return_value = {"status": "ok"}
        dataiku.get_dataset_row_count.return_value = 50
        fabric.query_row_count.return_value = None

        meta = {"incremental": {"column": "event_time", "last_watermark": "2024-01-01"}}
        asset = _make_asset(name="events", metadata=meta)
        ctx = self._make_context(dataiku_client=dataiku, fabric_client=fabric)

        result = asyncio.run(
            run_data_migration(ctx, asset, tmp_path)
        )

        assert result["status"] == "completed"
        assert result["export"]["incremental"] is True
        # Check incremental params forwarded to Dataiku client
        call_kwargs = dataiku.export_dataset_to_file.call_args
        assert call_kwargs.kwargs.get("filter_column") == "event_time"
        assert call_kwargs.kwargs.get("filter_value") == "2024-01-01"

    def test_csv_export_format(self, tmp_path: Path):
        dataiku = AsyncMock()
        fabric = AsyncMock()

        export_file = tmp_path / "items.csv"
        export_file.write_bytes(b"a,b\n1,2")
        dataiku.export_dataset_to_file.return_value = {
            "path": str(export_file),
            "bytes": 7,
            "incremental": False,
        }
        fabric.upload_to_lakehouse.return_value = {"status": "ok", "chunks": 1}
        fabric.load_table.return_value = {"status": "ok"}
        dataiku.get_dataset_row_count.return_value = 1

        cfg = StubConfig(migration=StubMigrationConfig(export_format="csv"))
        asset = _make_asset(name="items")
        ctx = self._make_context(dataiku_client=dataiku, fabric_client=fabric, config=cfg)

        result = asyncio.run(
            run_data_migration(ctx, asset, tmp_path)
        )

        assert result["status"] == "completed"
        # upload destination should use csv extension
        upload_call = fabric.upload_to_lakehouse.call_args
        assert "csv" in upload_call[0][2] or "csv" in str(upload_call)

    def test_upload_method_azcopy(self, tmp_path: Path):
        dataiku = AsyncMock()
        fabric = AsyncMock()

        export_file = tmp_path / "big.parquet"
        export_file.write_bytes(b"x" * 512)
        dataiku.export_dataset_to_file.return_value = {
            "path": str(export_file),
            "bytes": 512,
            "incremental": False,
        }
        fabric.workspace_id = "ws-1"
        fabric.upload_via_azcopy.return_value = {"status": "ok", "method": "azcopy"}
        fabric.load_table.return_value = {"status": "ok"}
        dataiku.get_dataset_row_count.return_value = 10

        cfg = StubConfig(migration=StubMigrationConfig(upload_method="azcopy"))
        asset = _make_asset(name="big")
        ctx = self._make_context(dataiku_client=dataiku, fabric_client=fabric, config=cfg)

        result = asyncio.run(
            run_data_migration(ctx, asset, tmp_path)
        )

        assert result["status"] == "completed"
        fabric.upload_via_azcopy.assert_called_once()


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# 7. LARGE_FILE_THRESHOLD constant
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

class TestThreshold:
    def test_threshold_value(self):
        assert LARGE_FILE_THRESHOLD == 100 * 1024 * 1024

    def test_auto_azcopy_for_large_file(self, tmp_path: Path):
        """Files > threshold trigger azcopy when upload_method is not explicitly httpx."""
        data_file = tmp_path / "huge.parquet"
        # We can't create a 100MB file easily; instead patch os.path.getsize
        data_file.write_bytes(b"x" * 10)

        client = AsyncMock()
        client.workspace_id = "ws-1"
        client.upload_via_azcopy.return_value = {"status": "ok", "method": "azcopy"}

        with patch("os.path.getsize", return_value=LARGE_FILE_THRESHOLD + 1):
            result = asyncio.run(
                upload_dataset(
                    client, "lh-1", str(data_file), "Files/huge.parquet",
                    upload_method="auto",
                )
            )
        client.upload_via_azcopy.assert_called_once()

    def test_auto_httpx_for_small_file(self, tmp_path: Path):
        """Files < threshold use httpx even in auto mode."""
        data_file = tmp_path / "small.parquet"
        data_file.write_bytes(b"x" * 10)

        client = AsyncMock()
        client.upload_to_lakehouse.return_value = {"status": "ok", "chunks": 1}

        with patch("os.path.getsize", return_value=LARGE_FILE_THRESHOLD - 1):
            result = asyncio.run(
                upload_dataset(
                    client, "lh-1", str(data_file), "Files/small.parquet",
                    upload_method="auto",
                )
            )
        client.upload_to_lakehouse.assert_called_once()

