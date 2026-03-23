"""Connection mapper agent — maps Dataiku connections to Fabric equivalents."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.agents.base_agent import AgentResult, AgentStatus, BaseAgent, ValidationResult
from src.core.logger import get_logger
from src.models.asset import AssetType, MigrationState

logger = get_logger(__name__)

# ── Connection type mapping: Dataiku → Fabric ────────────────

CONNECTION_MAP: dict[str, dict[str, Any]] = {
    "oracle": {
        "fabric_type": "OnPremisesDataGateway",
        "notes": "Requires on-premises data gateway installation and configuration",
        "category": "database",
    },
    "postgresql": {
        "fabric_type": "OnPremisesDataGateway",
        "notes": "Requires on-premises data gateway installation and configuration",
        "category": "database",
    },
    "sqlserver": {
        "fabric_type": "DirectConnection",
        "notes": "Native Fabric SQL connection or gateway depending on network",
        "category": "database",
    },
    "mysql": {
        "fabric_type": "OnPremisesDataGateway",
        "notes": "Requires on-premises data gateway",
        "category": "database",
    },
    "s3": {
        "fabric_type": "OneLakeShortcut_S3",
        "notes": "Configure S3-compatible OneLake shortcut",
        "category": "cloud_storage",
    },
    "azure_blob": {
        "fabric_type": "OneLakeShortcut_ADLS",
        "notes": "Native ADLS Gen2 shortcut in OneLake",
        "category": "cloud_storage",
    },
    "azure_datalake": {
        "fabric_type": "OneLakeShortcut_ADLS",
        "notes": "Native ADLS Gen2 shortcut in OneLake",
        "category": "cloud_storage",
    },
    "hdfs": {
        "fabric_type": "OneLake_FileCopy",
        "notes": "Data must be migrated to OneLake — no live shortcut",
        "category": "hadoop",
    },
    "bigquery": {
        "fabric_type": "Pipeline_CopyActivity",
        "notes": "Use Fabric Data Pipeline with BigQuery connector",
        "category": "cloud_database",
    },
    "snowflake": {
        "fabric_type": "Pipeline_CopyActivity",
        "notes": "Use Fabric Data Pipeline with Snowflake connector",
        "category": "cloud_database",
    },
    "filesystem": {
        "fabric_type": "Lakehouse_Files",
        "notes": "Upload files to Lakehouse Files section",
        "category": "local",
    },
    "ftp": {
        "fabric_type": "Pipeline_CopyActivity",
        "notes": "Use Fabric Data Pipeline Copy Activity for FTP sources",
        "category": "remote",
    },
}


def _build_gateway_template(conn_type: str, params: dict) -> dict:
    """Build an On-Premises Data Gateway configuration template.

    Returns a dict with connection string template, required secrets,
    and manual setup steps.
    """
    host = params.get("host", "<HOST>")
    port = params.get("port", "<PORT>")
    database = params.get("database", "<DATABASE>")

    templates: dict[str, dict] = {
        "oracle": {
            "connection_string": f"Data Source={host}:{port}/{database};",
            "auth_type": "Basic",
            "required_secrets": ["username", "password"],
        },
        "postgresql": {
            "connection_string": f"Host={host};Port={port};Database={database};",
            "auth_type": "Basic",
            "required_secrets": ["username", "password"],
        },
        "mysql": {
            "connection_string": f"Server={host};Port={port};Database={database};",
            "auth_type": "Basic",
            "required_secrets": ["username", "password"],
        },
        "sqlserver": {
            "connection_string": f"Server={host},{port};Database={database};",
            "auth_type": "Windows",
            "required_secrets": ["username", "password"],
        },
    }
    return templates.get(conn_type, {
        "connection_string": f"{conn_type}://{host}:{port}/{database}",
        "auth_type": "Basic",
        "required_secrets": ["username", "password"],
    })


def _build_shortcut_template(conn_type: str, params: dict) -> dict:
    """Build a OneLake shortcut configuration for cloud storage connections."""
    if conn_type == "s3":
        return {
            "shortcut_type": "AmazonS3",
            "location": params.get("bucket", "<S3_BUCKET>"),
            "sub_path": params.get("path", "/"),
            "connection_id": params.get("connection_id", "<AWS_CONNECTION_ID>"),
            "required_secrets": ["aws_access_key_id", "aws_secret_access_key"],
            "steps": [
                "Create an AWS connection in Fabric workspace settings",
                "Create a OneLake shortcut pointing to the S3 bucket",
                "Verify data accessibility through Lakehouse explorer",
            ],
        }
    if conn_type in ("azure_blob", "azure_datalake"):
        storage_account = params.get("account", "<STORAGE_ACCOUNT>")
        container = params.get("container", "<CONTAINER>")
        return {
            "shortcut_type": "ADLS Gen2",
            "location": f"https://{storage_account}.dfs.core.windows.net/{container}",
            "sub_path": params.get("path", "/"),
            "connection_id": params.get("connection_id", "<ADLS_CONNECTION_ID>"),
            "required_secrets": [],
            "steps": [
                "Ensure the storage account has hierarchical namespace enabled",
                "Create a OneLake shortcut to ADLS Gen2 container",
                "Use managed identity or service principal for auth",
            ],
        }
    return {}


def _build_pipeline_template(conn_type: str, params: dict) -> dict:
    """Build a Data Pipeline Copy Activity template for external data sources."""
    return {
        "activity_type": "Copy",
        "source_type": conn_type,
        "source_config": {
            "host": params.get("host", "<HOST>"),
            "database": params.get("database", "<DATABASE>"),
        },
        "sink_type": "Lakehouse",
        "required_secrets": _get_required_secrets(conn_type),
        "steps": [
            f"Create a {conn_type} linked service in Fabric workspace",
            "Configure the Data Pipeline with a Copy Activity",
            "Map source tables/files to Lakehouse Delta tables",
            "Test the pipeline with a small data sample first",
        ],
    }


def _get_required_secrets(conn_type: str) -> list[str]:
    """Return the list of required secrets for a connection type."""
    db_secrets = ["username", "password"]
    cloud_secrets = {
        "s3": ["aws_access_key_id", "aws_secret_access_key"],
        "azure_blob": [],  # Managed identity
        "azure_datalake": [],
        "bigquery": ["service_account_json"],
        "snowflake": ["username", "password", "account"],
        "ftp": ["username", "password"],
    }
    return cloud_secrets.get(conn_type, db_secrets)


def build_connection_config(
    asset_id: str,
    conn_type: str,
    mapping: dict,
    workspace_id: str,
    params: dict | None = None,
) -> dict:
    """Build a complete connection mapping configuration.

    Includes the Fabric equivalent type, credential templates,
    manual setup steps, and relevant configuration details.
    """
    params = params or {}
    fabric_type = mapping["fabric_type"]

    config: dict[str, Any] = {
        "connection_id": asset_id,
        "dataiku_type": conn_type,
        "fabric_type": fabric_type,
        "category": mapping.get("category", "unknown"),
        "notes": mapping["notes"],
        "workspace_id": workspace_id,
        "required_secrets": _get_required_secrets(conn_type),
        "manual_steps": [],
    }

    # Add type-specific templates
    if "Gateway" in fabric_type:
        config["gateway_config"] = _build_gateway_template(conn_type, params)
        config["manual_steps"] = [
            "Install and configure On-Premises Data Gateway",
            "Register gateway in Fabric workspace",
            "Create gateway data source with appropriate credentials",
            "Test the connection through the gateway",
        ]
    elif "Shortcut" in fabric_type:
        config["shortcut_config"] = _build_shortcut_template(conn_type, params)
        shortcut = config["shortcut_config"]
        config["manual_steps"] = shortcut.get("steps", [])
    elif fabric_type == "Pipeline_CopyActivity":
        config["pipeline_config"] = _build_pipeline_template(conn_type, params)
        config["manual_steps"] = config["pipeline_config"].get("steps", [])
    elif fabric_type == "Lakehouse_Files":
        config["manual_steps"] = [
            "Upload local files to Lakehouse Files section",
            "Convert to Delta tables if structured data",
        ]
    elif fabric_type == "DirectConnection":
        config["manual_steps"] = [
            "Verify SQL Server is accessible from Fabric workspace",
            "Configure connection in Fabric workspace settings",
        ]
    elif fabric_type == "OneLake_FileCopy":
        config["manual_steps"] = [
            "Export data from HDFS to local/cloud staging area",
            "Upload to OneLake Lakehouse Files section",
            "Convert to Delta tables if needed",
        ]

    return config


class ConnectionMapperAgent(BaseAgent):
    """Maps Dataiku connections to Fabric equivalents."""

    @property
    def name(self) -> str:
        return "connection_mapper"

    @property
    def description(self) -> str:
        return "Map Dataiku connections to Fabric equivalents (Gateway, OneLake, etc.)"

    async def execute(self, context: Any) -> AgentResult:
        registry = context.registry
        config = context.config
        output_dir = Path(config.migration.output_dir) / "connections"
        output_dir.mkdir(parents=True, exist_ok=True)

        connection_assets = registry.get_by_type(AssetType.CONNECTION)
        processed = 0
        converted = 0
        failed = 0
        all_flags: list[str] = []
        errors: list[str] = []

        for asset in connection_assets:
            registry.update_state(asset.id, MigrationState.CONVERTING)
            conn_type = asset.metadata.get("type", "").lower()
            conn_params = asset.metadata.get("params", {})

            mapping = CONNECTION_MAP.get(conn_type)
            if not mapping:
                flag = f"Unknown connection type '{conn_type}' — requires manual mapping"
                all_flags.append(flag)
                registry.add_review_flag(asset.id, flag)
                registry.update_state(asset.id, MigrationState.FAILED)
                failed += 1
                processed += 1
                continue

            try:
                connection_config = build_connection_config(
                    asset_id=asset.id,
                    conn_type=conn_type,
                    mapping=mapping,
                    workspace_id=config.fabric.workspace_id,
                    params=conn_params,
                )

                out_file = output_dir / f"{asset.name}.json"
                out_file.write_text(json.dumps(connection_config, indent=2), encoding="utf-8")

                registry.set_target(asset.id, {
                    "type": "connection_mapping",
                    "fabric_type": mapping["fabric_type"],
                    "category": mapping.get("category", "unknown"),
                    "path": str(out_file),
                    "manual_steps_count": len(connection_config["manual_steps"]),
                })
                registry.update_state(asset.id, MigrationState.CONVERTED)
                converted += 1

            except Exception as e:
                logger.error("connection_mapping_error", asset=asset.id, error=str(e))
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
            review_flags=all_flags,
            errors=errors,
        )

    async def validate(self, context: Any) -> ValidationResult:
        registry = context.registry
        converted = registry.get_by_type(AssetType.CONNECTION)
        converted = [a for a in converted if a.state == MigrationState.CONVERTED]

        checks_run = 0
        failures: list[str] = []

        for asset in converted:
            target = asset.target_fabric_asset or {}
            config_path = target.get("path")

            # Check config file exists
            checks_run += 1
            if not config_path or not Path(config_path).exists():
                failures.append(f"{asset.name}: Connection config not found")
                continue

            # Check config is valid JSON with required fields
            checks_run += 1
            try:
                config_data = json.loads(Path(config_path).read_text(encoding="utf-8"))
                required_keys = {"connection_id", "dataiku_type", "fabric_type", "workspace_id"}
                missing = required_keys - set(config_data.keys())
                if missing:
                    failures.append(f"{asset.name}: Config missing keys: {missing}")
            except json.JSONDecodeError:
                failures.append(f"{asset.name}: Config file is not valid JSON")

        return ValidationResult(
            passed=len(failures) == 0,
            checks_run=checks_run,
            checks_passed=checks_run - len(failures),
            checks_failed=len(failures),
            failures=failures,
        )
