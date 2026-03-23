"""Tests for the Dataiku REST API client."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from src.connectors.dataiku_client import DataikuClient

BASE_URL = "https://dss.example.com"
API_KEY = "test_api_key_123"


@pytest.fixture
def client():
    return DataikuClient(base_url=BASE_URL, api_key=API_KEY, timeout=5, max_retries=2)


class TestClientInit:
    def test_base_url_strips_trailing_slash(self):
        c = DataikuClient(base_url="https://dss.example.com/", api_key="k")
        assert c.base_url == "https://dss.example.com"

    def test_api_key_stored(self):
        c = DataikuClient(base_url=BASE_URL, api_key="secret")
        assert c._api_key == "secret"


class TestGetProject:
    @pytest.mark.asyncio
    async def test_get_project_success(self, client):
        mock_response = {"projectKey": "PROJ", "name": "Test Project"}
        with patch.object(client, "_request", new_callable=AsyncMock, return_value=mock_response):
            result = await client.get_project("PROJ")
            assert result["projectKey"] == "PROJ"


class TestListRecipes:
    @pytest.mark.asyncio
    async def test_list_recipes_returns_list(self, client):
        mock_data = [{"name": "r1", "type": "sql"}, {"name": "r2", "type": "python"}]
        with patch.object(client, "_paginated_list", new_callable=AsyncMock, return_value=mock_data):
            result = await client.list_recipes("PROJ")
            assert len(result) == 2
            assert result[0]["name"] == "r1"


class TestListDatasets:
    @pytest.mark.asyncio
    async def test_list_datasets_returns_list(self, client):
        mock_data = [{"name": "ds1"}, {"name": "ds2"}, {"name": "ds3"}]
        with patch.object(client, "_paginated_list", new_callable=AsyncMock, return_value=mock_data):
            result = await client.list_datasets("PROJ")
            assert len(result) == 3


class TestGetRecipe:
    @pytest.mark.asyncio
    async def test_get_recipe_returns_detail(self, client):
        mock_detail = {
            "name": "sql_recipe",
            "type": "sql",
            "payload": "SELECT 1",
            "inputs": {"main": {"items": [{"ref": "ds1"}]}},
            "outputs": {"main": {"items": [{"ref": "ds2"}]}},
        }
        with patch.object(client, "_request", new_callable=AsyncMock, return_value=mock_detail):
            result = await client.get_recipe("PROJ", "sql_recipe")
            assert result["payload"] == "SELECT 1"


class TestGetDatasetSchema:
    @pytest.mark.asyncio
    async def test_returns_schema(self, client):
        mock_schema = {"columns": [{"name": "id", "type": "int"}]}
        with patch.object(client, "_request", new_callable=AsyncMock, return_value=mock_schema):
            result = await client.get_dataset_schema("PROJ", "ds1")
            assert len(result["columns"]) == 1


class TestListConnections:
    @pytest.mark.asyncio
    async def test_handles_dict_response(self, client):
        # Admin connections endpoint returns dict keyed by name
        mock_dict = {
            "pg_main": {"type": "PostgreSQL", "params": {}},
            "oracle_dw": {"type": "Oracle", "params": {}},
        }
        with patch.object(client, "_request", new_callable=AsyncMock, return_value=mock_dict):
            result = await client.list_connections()
            assert isinstance(result, list)
            assert len(result) == 2
            names = {c["name"] for c in result}
            assert "pg_main" in names
            assert "oracle_dw" in names

    @pytest.mark.asyncio
    async def test_handles_list_response(self, client):
        mock_list = [{"name": "conn1"}, {"name": "conn2"}]
        with patch.object(client, "_request", new_callable=AsyncMock, return_value=mock_list):
            result = await client.list_connections()
            assert len(result) == 2


class TestPagination:
    @pytest.mark.asyncio
    async def test_single_page(self, client):
        """When results < limit, return them without further requests."""
        small_list = [{"name": f"item_{i}"} for i in range(5)]
        with patch.object(client, "_request", new_callable=AsyncMock, return_value=small_list):
            result = await client._paginated_list("/test/path", limit=100)
            assert len(result) == 5

    @pytest.mark.asyncio
    async def test_multi_page(self, client):
        """Simulate pagination with multiple calls."""
        page1 = [{"name": f"item_{i}"} for i in range(10)]
        page2 = [{"name": f"item_{i}"} for i in range(10, 15)]

        call_count = 0

        async def _mock_request(method, path, **kwargs):
            nonlocal call_count
            params = kwargs.get("params", {})
            offset = params.get("offset", 0)
            call_count += 1
            if offset == 0:
                return page1
            return page2

        with patch.object(client, "_request", side_effect=_mock_request):
            result = await client._paginated_list("/test/path", limit=10)
            assert len(result) == 15
            assert call_count == 2

    @pytest.mark.asyncio
    async def test_dict_response_with_total(self, client):
        """Handle paginated dict response with totalCount."""
        page1 = {"items": [{"id": 1}, {"id": 2}], "totalCount": 3}
        page2 = {"items": [{"id": 3}], "totalCount": 3}

        call_count = 0

        async def _mock_request(method, path, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return page1
            return page2

        with patch.object(client, "_request", side_effect=_mock_request):
            result = await client._paginated_list("/test/path", limit=2)
            assert len(result) == 3


class TestListDashboards:
    @pytest.mark.asyncio
    async def test_returns_empty_on_404(self, client):
        """Dashboards endpoint might not exist on older DSS versions."""
        error_response = httpx.Response(404, request=httpx.Request("GET", "https://x.com"))
        with patch.object(
            client, "_paginated_list",
            new_callable=AsyncMock,
            side_effect=httpx.HTTPStatusError("Not found", request=error_response.request, response=error_response),
        ):
            result = await client.list_dashboards("PROJ")
            assert result == []


class TestClientCleanup:
    @pytest.mark.asyncio
    async def test_close_client(self, client):
        # Ensure close doesn't raise even if client was never used
        await client.close()
