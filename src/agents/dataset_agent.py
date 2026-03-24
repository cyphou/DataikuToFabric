"""Dataset migration agent — migrates schemas and data to Lakehouse or Warehouse."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent, ValidationResult
from src.core.logger import get_logger
from src.models.asset import AssetType, MigrationState

logger = get_logger(__name__)

# ── Type mappings: Dataiku → Lakehouse (Spark SQL) ────────────

LAKEHOUSE_TYPE_MAP: dict[str, str] = {
    "string": "STRING",
    "tinyint": "TINYINT",
    "smallint": "SMALLINT",
    "int": "INT",
    "bigint": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "decimal": "DECIMAL",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
    "binary": "BINARY",
    "array": "ARRAY<STRING>",
    "map": "MAP<STRING,STRING>",
    "object": "STRING",
}

# ── Type mappings: Dataiku → Warehouse (T-SQL) ───────────────

WAREHOUSE_TYPE_MAP: dict[str, str] = {
    "string": "NVARCHAR(MAX)",
    "tinyint": "TINYINT",
    "smallint": "SMALLINT",
    "int": "INT",
    "bigint": "BIGINT",
    "float": "FLOAT",
    "double": "FLOAT",
    "decimal": "DECIMAL",
    "boolean": "BIT",
    "date": "DATE",
    "timestamp": "DATETIME2",
    "binary": "VARBINARY(MAX)",
    "array": "NVARCHAR(MAX)",
    "map": "NVARCHAR(MAX)",
    "object": "NVARCHAR(MAX)",
}


def decide_storage(dataset_metadata: dict, default: str = "lakehouse") -> str:
    """Decide whether a dataset should go to Lakehouse or Warehouse.

    Logic:
    - SQL-based sources (oracle, postgresql, sqlserver, sql_table) → warehouse
    - File-based sources (filesystem, s3, hdfs, azure_blob, azure_datalake) → lakehouse
    - Explicit ``storage`` hint in metadata overrides detection.
    """
    # Allow explicit override via metadata
    explicit = dataset_metadata.get("storage")
    if explicit in ("lakehouse", "warehouse"):
        return explicit

    ds_type = dataset_metadata.get("type", "").lower()
    connection_type = dataset_metadata.get("params", {}).get("connection", "").lower()

    # SQL-based datasets → Warehouse
    if ds_type in ("sql_table", "sqlserver", "oracle", "postgresql"):
        return "warehouse"
    # Check connection type as secondary signal
    if connection_type in ("oracle", "postgresql", "sqlserver", "mysql"):
        return "warehouse"
    # File-based → Lakehouse
    if ds_type in ("filesystem", "s3", "hdfs", "azure_blob", "azure_datalake"):
        return "lakehouse"
    return default


def _map_column_type(col: dict, target: str) -> str:
    """Map a single Dataiku column type to the target SQL type.

    Handles precision/length suffixes: ``decimal(18,2)`` → ``DECIMAL(18,2)``,
    ``string(255)`` → ``NVARCHAR(255)`` (warehouse) / ``STRING`` (lakehouse).
    """
    type_map = LAKEHOUSE_TYPE_MAP if target == "lakehouse" else WAREHOUSE_TYPE_MAP
    raw_type = col.get("type", "string").lower().strip()

    # Check for precision/length: e.g. "decimal(18,2)", "string(255)"
    base_type = raw_type
    precision = ""
    if "(" in raw_type:
        paren_idx = raw_type.index("(")
        base_type = raw_type[:paren_idx].strip()
        precision = raw_type[paren_idx:]  # includes parens

    mapped = type_map.get(base_type, type_map.get("string", "STRING"))

    # Apply precision if present
    if precision:
        if base_type == "decimal":
            return f"DECIMAL{precision}"
        if base_type == "string" and target == "warehouse":
            # Extract length for NVARCHAR
            length = precision.strip("()")
            return f"NVARCHAR({length})"
    # Use maxLength hint from schema metadata
    max_length = col.get("maxLength")
    if max_length and base_type == "string" and target == "warehouse":
        return f"NVARCHAR({max_length})"

    return mapped


def generate_ddl(
    table_name: str,
    columns: list[dict],
    target: str = "lakehouse",
    partition_by: list[str] | None = None,
) -> str:
    """Generate CREATE TABLE DDL for the target platform.

    Args:
        table_name: Target table name.
        columns: List of column dicts with ``name``, ``type``, ``nullable``, etc.
        target: ``"lakehouse"`` (Spark SQL / Delta) or ``"warehouse"`` (T-SQL).
        partition_by: Optional partition columns for Delta tables.
    """
    col_defs = []
    for col in columns:
        col_name = col.get("name", "unknown")
        mapped_type = _map_column_type(col, target)
        nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
        col_defs.append(f"  [{col_name}] {mapped_type} {nullable}")

    cols_sql = ",\n".join(col_defs)

    if target == "lakehouse":
        ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n{cols_sql}\n) USING DELTA"
        if partition_by:
            parts = ", ".join(partition_by)
            ddl += f"\nPARTITIONED BY ({parts})"
        return ddl
    else:
        return f"CREATE TABLE [{table_name}] (\n{cols_sql}\n)"


def compare_schemas(
    source_columns: list[dict],
    target_columns: list[dict],
) -> list[str]:
    """Compare source and target schemas and report differences.

    Returns a list of difference descriptions.  Empty list means matching.
    """
    diffs: list[str] = []

    src_names = [c.get("name", "").lower() for c in source_columns]
    tgt_names = [c.get("name", "").lower() for c in target_columns]

    src_set = set(src_names)
    tgt_set = set(tgt_names)

    for col in sorted(src_set - tgt_set):
        diffs.append(f"Column '{col}' in source but missing from target")
    for col in sorted(tgt_set - src_set):
        diffs.append(f"Column '{col}' in target but missing from source")

    # Compare types for common columns
    src_map = {c.get("name", "").lower(): c for c in source_columns}
    tgt_map = {c.get("name", "").lower(): c for c in target_columns}
    for col_name in sorted(src_set & tgt_set):
        src_type = src_map[col_name].get("type", "string").lower()
        tgt_type = tgt_map[col_name].get("type", "string").lower()
        if src_type != tgt_type:
            diffs.append(f"Column '{col_name}' type mismatch: source={src_type}, target={tgt_type}")

    return diffs


def generate_export_manifest(
    asset_name: str,
    columns: list[dict],
    storage: str,
    export_format: str = "parquet",
) -> dict:
    """Generate an export manifest describing how data should be moved.

    This manifest is consumed by the orchestrator to drive the actual data
    transfer (Dataiku export → local file → OneLake upload).
    """
    return {
        "asset_name": asset_name,
        "storage": storage,
        "export_format": export_format,
        "column_count": len(columns),
        "columns": [c.get("name", "unknown") for c in columns],
        "target_path": f"Tables/{asset_name}" if storage == "lakehouse" else asset_name,
    }


class DatasetMigrationAgent(BaseAgent):
    """Migrates dataset schemas to Lakehouse (Delta) or Warehouse tables."""

    @property
    def name(self) -> str:
        return "dataset_migrator"

    @property
    def description(self) -> str:
        return "Migrate Dataiku dataset schemas and data to Fabric Lakehouse or Warehouse"

    async def execute(self, context: Any) -> AgentResult:
        registry = context.registry
        config = context.config
        output_dir = Path(config.migration.output_dir) / "ddl"
        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_dir = Path(config.migration.output_dir) / "manifests"
        manifest_dir.mkdir(parents=True, exist_ok=True)

        dataset_assets = registry.get_by_type(AssetType.DATASET)
        processed = 0
        converted = 0
        failed = 0
        errors: list[str] = []
        review_flags: list[str] = []

        for asset in dataset_assets:
            registry.update_state(asset.id, MigrationState.CONVERTING)

            try:
                schema = asset.metadata.get("schema", {})
                columns = schema.get("columns", [])

                if not columns:
                    flag = f"{asset.name}: No columns in schema — DDL will be empty"
                    review_flags.append(flag)
                    registry.add_review_flag(asset.id, flag)

                storage = decide_storage(asset.metadata, config.migration.default_storage)

                # Extract partition hints from metadata if available
                partition_by = asset.metadata.get("partitioning", {}).get("columns")

                ddl = generate_ddl(asset.name, columns, target=storage,
                                   partition_by=partition_by if storage == "lakehouse" else None)

                out_file = output_dir / f"{asset.name}.sql"
                out_file.write_text(ddl, encoding="utf-8")

                # Generate export manifest for data migration
                manifest = generate_export_manifest(asset.name, columns, storage)
                manifest_file = manifest_dir / f"{asset.name}.json"
                manifest_file.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

                registry.set_target(asset.id, {
                    "type": f"{storage}_table",
                    "storage": storage,
                    "ddl_path": str(out_file),
                    "manifest_path": str(manifest_file),
                    "column_count": len(columns),
                    "target_table": manifest["target_path"],
                })
                registry.update_state(asset.id, MigrationState.CONVERTED)
                converted += 1

            except Exception as e:
                logger.error("dataset_migration_error", asset=asset.id, error=str(e))
                registry.add_error(asset.id, str(e))
                registry.update_state(asset.id, MigrationState.FAILED)
                errors.append(f"{asset.name}: {e}")
                failed += 1

            processed += 1

        return AgentResult(
            agent_name=self.name,
            status=AgentStatus.COMPLETED,
            assets_processed=processed,
            assets_converted=converted,
            assets_failed=failed,
            review_flags=review_flags,
            errors=errors,
        )

    async def validate(self, context: Any) -> ValidationResult:
        registry = context.registry
        converted = registry.get_by_type(AssetType.DATASET)
        converted = [a for a in converted if a.state == MigrationState.CONVERTED]

        checks_run = 0
        failures: list[str] = []

        for asset in converted:
            target = asset.target_fabric_asset or {}

            # Check DDL file existence
            ddl_path = target.get("ddl_path") or target.get("path")
            checks_run += 1
            if not ddl_path or not Path(ddl_path).exists():
                failures.append(f"{asset.name}: DDL file not found")
                continue

            # Check DDL is non-empty and contains CREATE TABLE
            ddl_content = Path(ddl_path).read_text(encoding="utf-8")
            checks_run += 1
            if "CREATE TABLE" not in ddl_content:
                failures.append(f"{asset.name}: DDL missing CREATE TABLE statement")

            # Check column count matches
            schema = asset.metadata.get("schema", {})
            expected_cols = len(schema.get("columns", []))
            actual_cols = target.get("column_count", 0)
            checks_run += 1
            if expected_cols > 0 and actual_cols != expected_cols:
                failures.append(
                    f"{asset.name}: Column count mismatch — expected {expected_cols}, got {actual_cols}"
                )

            # Check manifest file existence
            manifest_path = target.get("manifest_path")
            if manifest_path:
                checks_run += 1
                if not Path(manifest_path).exists():
                    failures.append(f"{asset.name}: Export manifest not found")

        return ValidationResult(
            passed=len(failures) == 0,
            checks_run=checks_run,
            checks_passed=checks_run - len(failures),
            checks_failed=len(failures),
            failures=failures,
        )


# ── Data migration helpers ────────────────────────────────────

LARGE_FILE_THRESHOLD = 100 * 1024 * 1024  # 100 MB


async def export_dataset(
    dataiku_client: Any,
    project_key: str,
    dataset_name: str,
    output_dir: Path,
    fmt: str = "parquet",
    *,
    incremental_column: str | None = None,
    watermark_value: str | None = None,
) -> dict:
    """Export a dataset from Dataiku to a local file.

    Args:
        dataiku_client: DataikuClient instance.
        project_key: Source project.
        dataset_name: Dataset to export.
        output_dir: Local staging directory.
        fmt: Export format (``parquet``, ``csv``).
        incremental_column: Column for watermark-based incremental export.
        watermark_value: Previous watermark value (export rows > this).

    Returns export metadata dict.
    """
    ext = "parquet" if "parquet" in fmt else "csv"
    output_path = str(output_dir / f"{dataset_name}.{ext}")
    result = await dataiku_client.export_dataset_to_file(
        project_key, dataset_name, output_path, fmt=fmt,
        filter_column=incremental_column,
        filter_value=watermark_value,
    )
    logger.info("dataset_exported", dataset=dataset_name, **result)
    return result


async def upload_dataset(
    fabric_client: Any,
    lakehouse_id: str,
    local_path: str,
    destination_path: str,
    *,
    chunk_size_mb: int = 4,
    upload_method: str = "httpx",
    on_progress: Any | None = None,
) -> dict:
    """Upload an exported dataset file to OneLake.

    Automatically uses azcopy for files >100 MB if configured.

    Args:
        fabric_client: FabricClient instance.
        lakehouse_id: Target Lakehouse item ID.
        local_path: Path to local file.
        destination_path: Destination in OneLake (e.g. ``Files/data.parquet``).
        chunk_size_mb: Chunk size for httpx upload.
        upload_method: ``httpx`` or ``azcopy``.
        on_progress: Optional progress callback.
    """
    file_size = os.path.getsize(local_path)

    # Use azcopy for large files or when explicitly requested
    if upload_method == "azcopy" or (file_size > LARGE_FILE_THRESHOLD and upload_method != "httpx"):
        try:
            dest_url = (
                f"https://onelake.dfs.fabric.microsoft.com"
                f"/{fabric_client.workspace_id}/{lakehouse_id}/{destination_path}"
            )
            result = await fabric_client.upload_via_azcopy(local_path, dest_url)
            logger.info("dataset_uploaded_azcopy", destination=destination_path, size=file_size)
            return result
        except RuntimeError:
            logger.warning("azcopy_fallback", reason="azcopy not available, using httpx")
            # Fall through to httpx upload

    result = await fabric_client.upload_to_lakehouse(
        lakehouse_id, local_path, destination_path,
        chunk_size_mb=chunk_size_mb, on_progress=on_progress,
    )
    logger.info("dataset_uploaded", destination=destination_path, **result)
    return result


async def load_into_table(
    fabric_client: Any,
    lakehouse_id: str,
    table_name: str,
    file_path_in_lakehouse: str,
    file_format: str = "parquet",
    mode: str = "overwrite",
) -> dict:
    """Load staged data from Lakehouse Files into a Delta table.

    Args:
        fabric_client: FabricClient instance.
        lakehouse_id: Target Lakehouse.
        table_name: Delta table name.
        file_path_in_lakehouse: Relative path within Lakehouse (e.g. ``Files/data.parquet``).
        file_format: Source file format.
        mode: ``overwrite`` or ``append``.
    """
    result = await fabric_client.load_table(
        lakehouse_id, table_name, file_path_in_lakehouse,
        file_format=file_format, mode=mode,
    )
    logger.info("table_loaded", table=table_name, mode=mode)
    return result


async def verify_row_counts(
    dataiku_client: Any,
    fabric_client: Any,
    project_key: str,
    dataset_name: str,
    warehouse_id: str | None = None,
    table_name: str | None = None,
) -> dict:
    """Compare row counts between source (Dataiku) and target (Fabric).

    Returns a dict with source_count, target_count, and match status.
    """
    source_count = await dataiku_client.get_dataset_row_count(project_key, dataset_name)

    target_count = None
    if warehouse_id and table_name:
        target_count = await fabric_client.query_row_count(warehouse_id, table_name)

    match = (
        source_count is not None
        and target_count is not None
        and source_count == target_count
    )
    result = {
        "dataset": dataset_name,
        "source_count": source_count,
        "target_count": target_count,
        "match": match,
        "source_available": source_count is not None,
        "target_available": target_count is not None,
    }

    if not match and source_count is not None and target_count is not None:
        logger.warning("row_count_mismatch", **result)
    elif match:
        logger.info("row_count_verified", **result)

    return result


def get_watermark(asset_metadata: dict) -> tuple[str | None, str | None]:
    """Extract incremental watermark config from asset metadata.

    Returns (column_name, last_watermark_value) or (None, None).
    """
    incremental = asset_metadata.get("incremental", {})
    column = incremental.get("column")
    watermark = incremental.get("last_watermark")
    return column, watermark


def update_watermark(asset_metadata: dict, column: str, value: str) -> None:
    """Store watermark value in asset metadata for next incremental run."""
    if "incremental" not in asset_metadata:
        asset_metadata["incremental"] = {}
    asset_metadata["incremental"]["column"] = column
    asset_metadata["incremental"]["last_watermark"] = value


async def run_data_migration(
    context: Any,
    asset: Any,
    staging_dir: Path,
) -> dict:
    """Execute the full data migration pipeline for a single dataset asset.

    Steps: export → upload → load → verify row counts.

    Returns a result dict with status and details from each step.
    """
    config = context.config
    dataiku_client = context.connectors.get("dataiku")
    fabric_client = context.connectors.get("fabric")

    if not dataiku_client or not fabric_client:
        return {"status": "skipped", "reason": "connectors not configured"}

    project_key = config.dataiku.project_key
    target = asset.target_fabric_asset or {}
    storage = target.get("storage", "lakehouse")
    table_name = asset.name
    export_format = config.migration.export_format

    # Check for incremental watermark
    inc_col, inc_val = get_watermark(asset.metadata)

    # Step 1: Export from Dataiku
    export_result = await export_dataset(
        dataiku_client, project_key, table_name, staging_dir,
        fmt=export_format,
        incremental_column=inc_col,
        watermark_value=inc_val,
    )

    local_path = export_result["path"]
    ext = "parquet" if "parquet" in export_format else "csv"
    dest_path = f"Files/staging/{table_name}.{ext}"

    # Step 2: Upload to OneLake
    lakehouse_id = getattr(config.fabric, "lakehouse_id", config.fabric.workspace_id)
    upload_result = await upload_dataset(
        fabric_client, lakehouse_id, local_path, dest_path,
        chunk_size_mb=config.migration.chunk_size_mb,
        upload_method=config.migration.upload_method,
    )

    # Step 3: Load into Delta table (lakehouse only)
    load_result = {}
    if storage == "lakehouse":
        load_result = await load_into_table(
            fabric_client, lakehouse_id, table_name, dest_path,
            file_format=ext, mode=config.migration.load_mode,
        )

    # Step 4: Verify row counts
    warehouse_id = getattr(config.fabric, "warehouse_id", None)
    verify_result = await verify_row_counts(
        dataiku_client, fabric_client,
        project_key, table_name,
        warehouse_id=warehouse_id if storage == "warehouse" else None,
        table_name=table_name,
    )

    return {
        "status": "completed",
        "export": export_result,
        "upload": upload_result,
        "load": load_result,
        "row_count_verification": verify_result,
    }
