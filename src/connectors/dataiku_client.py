"""Dataiku REST API client — connects to Dataiku DSS for asset discovery."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from src.core.logger import get_logger

logger = get_logger(__name__)


class DataikuClient:
    """Async client for the Dataiku DSS Public REST API.

    Auth: Dataiku uses personal API keys passed as a query parameter.
    Reference: https://doc.dataiku.com/dss/latest/publicapi/rest.html
    """

    def __init__(self, base_url: str, api_key: str, timeout: int = 30, max_retries: int = 3):
        self.base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout = timeout
        self._max_retries = max_retries
        self._client: httpx.AsyncClient | None = None

    async def _ensure_client(self) -> httpx.AsyncClient:
        """Reuse a single httpx client (connection pool)."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self._timeout,
                verify=True,
                headers={"Content-Type": "application/json"},
            )
        return self._client

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        """Make an authenticated API request with retry + backoff."""
        url = f"{self.base_url}/public/api{path}"
        params = kwargs.pop("params", {})
        params["apiKey"] = self._api_key

        client = await self._ensure_client()

        for attempt in range(1, self._max_retries + 1):
            try:
                response = await client.request(
                    method, url, params=params, **kwargs
                )
                response.raise_for_status()
                if response.status_code == 204:
                    return {}
                return response.json()
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                if status == 429 and attempt < self._max_retries:
                    retry_after = int(e.response.headers.get("Retry-After", str(attempt * 2)))
                    logger.warning("rate_limited", url=url, retry_after=retry_after)
                    await asyncio.sleep(retry_after)
                    continue
                if status >= 500 and attempt < self._max_retries:
                    logger.warning("server_error", url=url, status=status, attempt=attempt)
                    await asyncio.sleep(attempt * 2)
                    continue
                raise
            except httpx.RequestError as e:
                if attempt < self._max_retries:
                    logger.warning("request_error", url=url, attempt=attempt, error=str(e))
                    await asyncio.sleep(attempt * 2)
                    continue
                raise
        raise RuntimeError(f"Request to {url} failed after {self._max_retries} retries")

    async def _paginated_list(self, path: str, *, limit: int = 100, **kwargs: Any) -> list[dict]:
        """Fetch a paginated list endpoint, accumulating all pages.

        Dataiku pagination uses `limit` + `offset` query params where supported.
        Falls back to a single request if the API doesn't paginate.
        """
        all_items: list[dict] = []
        offset = 0

        while True:
            params = kwargs.pop("params", {})
            params.update({"limit": limit, "offset": offset})
            data = await self._request("GET", path, params=params, **kwargs)

            # Dataiku can return a list directly or a dict with items/count
            if isinstance(data, list):
                all_items.extend(data)
                # If we got fewer than `limit`, we're done
                if len(data) < limit:
                    break
                offset += len(data)
            elif isinstance(data, dict):
                items = data.get("items", data.get("results", []))
                all_items.extend(items)
                total = data.get("totalCount", data.get("count"))
                if total is not None and offset + len(items) >= total:
                    break
                if len(items) < limit:
                    break
                offset += len(items)
            else:
                break

        return all_items

    # ── Project-level APIs ────────────────────────────────────

    async def get_project(self, project_key: str) -> dict:
        """Get project metadata."""
        return await self._request("GET", f"/projects/{project_key}")

    async def list_recipes(self, project_key: str) -> list[dict]:
        """List all recipes in a project."""
        return await self._paginated_list(f"/projects/{project_key}/recipes/")

    async def get_recipe(self, project_key: str, recipe_name: str) -> dict:
        """Get a specific recipe's full definition."""
        return await self._request("GET", f"/projects/{project_key}/recipes/{recipe_name}")

    async def list_datasets(self, project_key: str) -> list[dict]:
        """List all datasets in a project."""
        return await self._paginated_list(f"/projects/{project_key}/datasets/")

    async def get_dataset(self, project_key: str, dataset_name: str) -> dict:
        """Get a specific dataset's schema and metadata."""
        return await self._request("GET", f"/projects/{project_key}/datasets/{dataset_name}")

    async def get_dataset_schema(self, project_key: str, dataset_name: str) -> dict:
        """Get a dataset's schema definition."""
        return await self._request("GET", f"/projects/{project_key}/datasets/{dataset_name}/schema")

    async def list_managed_folders(self, project_key: str) -> list[dict]:
        """List all managed folders in a project."""
        return await self._paginated_list(f"/projects/{project_key}/managedfolders/")

    async def get_flow(self, project_key: str) -> dict:
        """Get the project flow graph (all nodes and edges)."""
        return await self._request("GET", f"/projects/{project_key}/flow/")

    async def list_scenarios(self, project_key: str) -> list[dict]:
        """List all scenarios in a project."""
        return await self._paginated_list(f"/projects/{project_key}/scenarios/")

    async def list_saved_models(self, project_key: str) -> list[dict]:
        """List all saved models in a project."""
        return await self._paginated_list(f"/projects/{project_key}/savedmodels/")

    async def list_dashboards(self, project_key: str) -> list[dict]:
        """List all dashboards in a project (if API supports it)."""
        try:
            return await self._paginated_list(f"/projects/{project_key}/dashboards/")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.info("dashboards_not_supported", project=project_key)
                return []
            raise

    # ── Admin APIs ────────────────────────────────────────────

    async def list_connections(self) -> list[dict]:
        """List all instance-level connections (requires admin API key)."""
        data = await self._request("GET", "/admin/connections/")
        # Admin connections endpoint returns a dict keyed by connection name
        if isinstance(data, dict) and not data.get("items"):
            return [{"name": k, **v} for k, v in data.items()]
        return data if isinstance(data, list) else []

    # ── Data export ───────────────────────────────────────────

    async def export_dataset(self, project_key: str, dataset_name: str, fmt: str = "csv") -> bytes:
        """Export dataset data in the specified format (csv, tsv).

        Returns raw bytes of the exported data.
        """
        url = f"{self.base_url}/public/api/projects/{project_key}/datasets/{dataset_name}/data"
        params = {"apiKey": self._api_key, "format": fmt}
        client = await self._ensure_client()
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.content

    async def export_dataset_to_file(
        self,
        project_key: str,
        dataset_name: str,
        output_path: str,
        fmt: str = "csv",
        *,
        filter_column: str | None = None,
        filter_value: str | None = None,
    ) -> dict:
        """Export dataset data to a local file using streaming download.

        Streams data in chunks to handle large datasets without
        loading everything into memory.

        Args:
            project_key: Dataiku project key.
            dataset_name: Dataset name to export.
            output_path: Local file path for output.
            fmt: Export format (``csv``, ``tsv``, ``parquet-stream``).
            filter_column: If set, export only rows where column > filter_value (watermark).
            filter_value: The watermark threshold value for incremental export.

        Returns metadata about the export (file size, row estimate).
        """
        from pathlib import Path

        url = f"{self.base_url}/public/api/projects/{project_key}/datasets/{dataset_name}/data"
        params: dict[str, str] = {"apiKey": self._api_key, "format": fmt}

        # Incremental filter — Dataiku DSS supports filter as a query param
        if filter_column and filter_value is not None:
            params["filter"] = f"{filter_column} > '{filter_value}'"

        client = await self._ensure_client()

        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        total_bytes = 0

        async with client.stream("GET", url, params=params) as response:
            response.raise_for_status()
            with open(out, "wb") as f:
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    f.write(chunk)
                    total_bytes += len(chunk)

        return {
            "path": str(out),
            "format": fmt,
            "size_bytes": total_bytes,
            "incremental": filter_column is not None,
        }

    async def get_dataset_row_count(self, project_key: str, dataset_name: str) -> int | None:
        """Get the row count for a dataset (from metrics if available).

        Returns None if the count is not available.
        """
        try:
            data = await self._request(
                "GET",
                f"/projects/{project_key}/datasets/{dataset_name}/metrics",
            )
            # Dataiku returns metrics as a nested structure
            if isinstance(data, dict):
                for metric in data.get("metrics", []):
                    if metric.get("metricId") == "records:COUNT_RECORDS":
                        value = metric.get("lastValues", [{}])
                        if value:
                            return int(value[0].get("value", 0))
            return None
        except Exception:
            return None
