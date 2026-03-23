"""Fabric REST API client — deploys assets to Microsoft Fabric workspaces."""

from __future__ import annotations

import asyncio
import base64
import json
import os
from typing import Any

import httpx

from src.core.logger import get_logger

logger = get_logger(__name__)

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"


def _acquire_token(config: Any) -> str:
    """Acquire an Azure AD token using the configured auth method.

    Supports: azure_cli, environment, token (pre-acquired).
    Falls back to DefaultAzureCredential if azure-identity is installed.
    """
    auth_method = getattr(config, "auth_method", "azure_cli")

    # Allow a pre-acquired token via env var for testing
    env_token = os.environ.get("FABRIC_ACCESS_TOKEN")
    if env_token:
        return env_token

    try:
        from azure.identity import DefaultAzureCredential

        credential = DefaultAzureCredential()
        token = credential.get_token(FABRIC_SCOPE)
        return token.token
    except ImportError:
        raise RuntimeError(
            "azure-identity package not installed. Install with: "
            "pip install azure-identity  or set FABRIC_ACCESS_TOKEN env var."
        )


class FabricClient:
    """Async client for the Microsoft Fabric REST API."""

    def __init__(self, workspace_id: str, access_token: str, timeout: int = 60):
        self.workspace_id = workspace_id
        self._timeout = timeout
        self._headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        """Make an authenticated API request with 202 long-running op polling."""
        url = f"{FABRIC_API_BASE}{path}"
        async with httpx.AsyncClient(timeout=self._timeout, verify=True) as client:
            response = await client.request(method, url, headers=self._headers, **kwargs)
            response.raise_for_status()
            if response.status_code == 204:
                return {}
            # Handle long-running operations (202 Accepted)
            if response.status_code == 202:
                return await self._poll_operation(client, response)
            return response.json()

    async def _poll_operation(self, client: httpx.AsyncClient, response: httpx.Response,
                              max_polls: int = 60, interval: int = 2) -> dict:
        """Poll a 202 Accepted operation until completion."""
        location = response.headers.get("Location")
        retry_after = int(response.headers.get("Retry-After", str(interval)))
        if not location:
            return response.json() if response.content else {}

        for _ in range(max_polls):
            await asyncio.sleep(retry_after)
            poll_resp = await client.get(location, headers=self._headers)
            if poll_resp.status_code == 200:
                return poll_resp.json() if poll_resp.content else {}
            if poll_resp.status_code == 202:
                retry_after = int(poll_resp.headers.get("Retry-After", str(interval)))
                continue
            poll_resp.raise_for_status()

        raise TimeoutError("Long-running operation did not complete in time")

    # ── Workspace APIs ────────────────────────────────────────

    async def list_items(self) -> list[dict]:
        """List all items in the workspace."""
        return await self._request("GET", f"/workspaces/{self.workspace_id}/items")

    async def create_item(self, display_name: str, item_type: str,
                          definition: dict | None = None) -> dict:
        """Create a new item (Lakehouse, Notebook, Pipeline, etc.)."""
        body: dict[str, Any] = {
            "displayName": display_name,
            "type": item_type,
        }
        if definition:
            body["definition"] = definition
        return await self._request(
            "POST", f"/workspaces/{self.workspace_id}/items", json=body
        )

    async def update_item_definition(self, item_id: str, definition: dict) -> dict:
        """Update an item's definition."""
        return await self._request(
            "POST",
            f"/workspaces/{self.workspace_id}/items/{item_id}/updateDefinition",
            json={"definition": definition},
        )

    # ── Lakehouse APIs ────────────────────────────────────────

    async def create_lakehouse(self, name: str) -> dict:
        """Create a new Lakehouse."""
        return await self.create_item(name, "Lakehouse")

    # ── Notebook APIs ─────────────────────────────────────────

    async def create_notebook(self, name: str, content: str) -> dict:
        """Create a Fabric Notebook with the given .ipynb content."""
        encoded = base64.b64encode(content.encode()).decode()
        definition = {
            "parts": [
                {
                    "path": "notebook-content.py",
                    "payload": encoded,
                    "payloadType": "InlineBase64",
                }
            ]
        }
        return await self.create_item(name, "Notebook", definition)

    # ── Pipeline APIs ─────────────────────────────────────────

    async def create_pipeline(self, name: str, pipeline_json: dict) -> dict:
        """Create a Fabric Data Pipeline."""
        encoded = base64.b64encode(json.dumps(pipeline_json).encode()).decode()
        definition = {
            "parts": [
                {
                    "path": "pipeline-content.json",
                    "payload": encoded,
                    "payloadType": "InlineBase64",
                }
            ]
        }
        return await self.create_item(name, "DataPipeline", definition)

    # ── SQL APIs ──────────────────────────────────────────────

    async def execute_sql(self, item_id: str, sql: str) -> dict:
        """Execute a SQL statement against a Warehouse."""
        return await self._request(
            "POST",
            f"/workspaces/{self.workspace_id}/warehouses/{item_id}/queries",
            json={"query": sql},
        )

    # ── Warehouse APIs ────────────────────────────────────────

    async def create_warehouse(self, name: str) -> dict:
        """Create a new Warehouse."""
        return await self.create_item(name, "Warehouse")

    async def get_warehouse(self, warehouse_id: str) -> dict:
        """Get warehouse details."""
        return await self._request(
            "GET", f"/workspaces/{self.workspace_id}/warehouses/{warehouse_id}"
        )

    # ── OneLake upload APIs ───────────────────────────────────

    async def upload_to_lakehouse(
        self,
        lakehouse_id: str,
        file_path: str,
        destination_path: str,
    ) -> dict:
        """Upload a file to Lakehouse Files section via the OneLake API.

        Uses the Fabric OneLake ADLS Gen2-compatible endpoint.

        Args:
            lakehouse_id: The Lakehouse item ID.
            file_path: Local file path to upload.
            destination_path: Destination path within OneLake (e.g. ``Files/data.parquet``).
        """
        import os

        onelake_url = (
            f"https://onelake.dfs.fabric.microsoft.com"
            f"/{self.workspace_id}/{lakehouse_id}/{destination_path}"
        )

        file_size = os.path.getsize(file_path)

        async with httpx.AsyncClient(timeout=self._timeout * 5, verify=True) as client:
            # Step 1: Create file
            create_resp = await client.put(
                onelake_url,
                headers={**self._headers, "Content-Length": "0"},
                params={"resource": "file"},
            )
            create_resp.raise_for_status()

            # Step 2: Append data
            with open(file_path, "rb") as f:
                data = f.read()

            append_resp = await client.patch(
                onelake_url,
                headers={
                    "Authorization": self._headers["Authorization"],
                    "Content-Length": str(file_size),
                },
                params={"action": "append", "position": "0"},
                content=data,
            )
            append_resp.raise_for_status()

            # Step 3: Flush to commit
            flush_resp = await client.patch(
                onelake_url,
                headers={**self._headers, "Content-Length": "0"},
                params={"action": "flush", "position": str(file_size)},
            )
            flush_resp.raise_for_status()

        return {
            "status": "uploaded",
            "destination": destination_path,
            "size_bytes": file_size,
        }

    async def list_lakehouse_tables(self, lakehouse_id: str) -> list[dict]:
        """List tables in a Lakehouse."""
        result = await self._request(
            "GET",
            f"/workspaces/{self.workspace_id}/lakehouses/{lakehouse_id}/tables",
        )
        return result.get("data", result) if isinstance(result, dict) else result

    async def load_table(
        self,
        lakehouse_id: str,
        table_name: str,
        relative_path: str,
        file_format: str = "parquet",
        mode: str = "overwrite",
    ) -> dict:
        """Load a file from Lakehouse Files into a Delta table.

        Args:
            lakehouse_id: The Lakehouse item ID.
            table_name: Target table name.
            relative_path: Path within Lakehouse Files (e.g. ``Files/data.parquet``).
            file_format: Source file format (``parquet``, ``csv``, ``json``).
            mode: Load mode (``overwrite`` or ``append``).
        """
        body = {
            "relativePath": relative_path,
            "pathType": "File",
            "mode": mode,
            "formatOptions": {"format": file_format},
        }
        return await self._request(
            "POST",
            f"/workspaces/{self.workspace_id}/lakehouses/{lakehouse_id}"
            f"/tables/{table_name}/load",
            json=body,
        )
