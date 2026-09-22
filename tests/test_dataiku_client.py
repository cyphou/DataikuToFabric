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

    def test_verify_ssl_disabled(self):
        c = DataikuClient(base_url=BASE_URL, api_key="secret", verify_ssl=False)
        assert c._verify is False

    def test_ca_bundle_path_overrides_verify_flag(self):
        c = DataikuClient(
            base_url=BASE_URL,
            api_key="secret",
            verify_ssl=False,
            ca_bundle_path="/tmp/custom-ca.pem",
        )
        assert c._verify == "/tmp/custom-ca.pem"


class TestGetProject:
    @pytest.mark.asyncio
    async def test_get_project_success(self, client):
        mock_response = {"projectKey": "PROJ", "name": "Test Project"}
        with patch.object(client, "_request", new_callable=AsyncMock, return_value=mock_response):
            result = await client.get_project("PROJ")
            assert result["projectKey"] == "PROJ"


class TestAuthentication:
    """Auth uses an Authorization: Bearer header, not an apiKey query param.

    Some Dataiku deployments (e.g. behind certain gateways/proxies) reject
    the legacy ``?apiKey=`` query-param form with a 401.
    """

    @pytest.mark.asyncio
    async def test_ensure_client_sets_bearer_header(self, client):
        http_client = await client._ensure_client()
        assert http_client.headers["Authorization"] == f"Bearer {API_KEY}"

    @pytest.mark.asyncio
    async def test_request_does_not_send_api_key_as_query_param(self, client):
        captured: dict = {}

        async def _fake_request(method, url, params=None, **kwargs):
            captured["params"] = params or {}
            return httpx.Response(
                200, json={"ok": True}, request=httpx.Request(method, url)
            )

        http_client = await client._ensure_client()
        with patch.object(http_client, "request", side_effect=_fake_request):
            await client._request("GET", "/projects/PROJ")

        assert "apiKey" not in captured["params"]


class TestConnectionSetup:
    """Redirect-following and proxy support for gateway-fronted deployments."""

    @pytest.mark.asyncio
    async def test_follow_redirects_enabled(self, client):
        http_client = await client._ensure_client()
        assert http_client.follow_redirects is True

    @pytest.mark.asyncio
    async def test_proxy_url_forwarded_to_httpx_client(self):
        c = DataikuClient(base_url=BASE_URL, api_key=API_KEY, proxy_url="http://proxy.corp:8080")
        with patch("src.connectors.dataiku_client.httpx.AsyncClient") as mock_cls:
            await c._ensure_client()
        assert mock_cls.call_args.kwargs["proxy"] == "http://proxy.corp:8080"

    @pytest.mark.asyncio
    async def test_no_proxy_configured_by_default(self, client):
        with patch("src.connectors.dataiku_client.httpx.AsyncClient") as mock_cls:
            await client._ensure_client()
        assert mock_cls.call_args.kwargs["proxy"] is None


class TestConnectionTest:
    """`test_connection()` classifies failures for actionable CLI diagnostics."""

    @pytest.mark.asyncio
    async def test_success(self, client):
        project = {"projectKey": "PROJ", "name": "Test"}
        with patch.object(client, "get_project", new_callable=AsyncMock, return_value=project):
            result = await client.test_connection("PROJ")
        assert result["success"] is True
        assert result["category"] == "ok"
        assert result["project"] == project

    @pytest.mark.asyncio
    async def test_unauthorized(self, client):
        resp = httpx.Response(401, request=httpx.Request("GET", "https://x.com"))
        err = httpx.HTTPStatusError("401", request=resp.request, response=resp)
        with patch.object(client, "get_project", new_callable=AsyncMock, side_effect=err):
            result = await client.test_connection("PROJ")
        assert result["success"] is False
        assert result["category"] == "unauthorized"

    @pytest.mark.asyncio
    async def test_forbidden(self, client):
        resp = httpx.Response(403, request=httpx.Request("GET", "https://x.com"))
        err = httpx.HTTPStatusError("403", request=resp.request, response=resp)
        with patch.object(client, "get_project", new_callable=AsyncMock, side_effect=err):
            result = await client.test_connection("PROJ")
        assert result["category"] == "forbidden"

    @pytest.mark.asyncio
    async def test_not_found(self, client):
        resp = httpx.Response(404, request=httpx.Request("GET", "https://x.com"))
        err = httpx.HTTPStatusError("404", request=resp.request, response=resp)
        with patch.object(client, "get_project", new_callable=AsyncMock, side_effect=err):
            result = await client.test_connection("PROJ")
        assert result["category"] == "not_found"

    @pytest.mark.asyncio
    async def test_other_http_error(self, client):
        resp = httpx.Response(500, request=httpx.Request("GET", "https://x.com"))
        err = httpx.HTTPStatusError("500", request=resp.request, response=resp)
        with patch.object(client, "get_project", new_callable=AsyncMock, side_effect=err):
            result = await client.test_connection("PROJ")
        assert result["category"] == "http_error"
        assert result["status"] == 500

    @pytest.mark.asyncio
    async def test_connection_error(self, client):
        err = httpx.ConnectError("refused", request=httpx.Request("GET", "https://x.com"))
        with patch.object(client, "get_project", new_callable=AsyncMock, side_effect=err):
            result = await client.test_connection("PROJ")
        assert result["category"] == "connection_error"

    @pytest.mark.asyncio
    async def test_ssl_error_gives_ca_bundle_hint(self, client):
        err = httpx.ConnectError(
            "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed",
            request=httpx.Request("GET", "https://x.com"),
        )
        with patch.object(client, "get_project", new_callable=AsyncMock, side_effect=err):
            result = await client.test_connection("PROJ")
        assert result["category"] == "connection_error"
        assert "ca_bundle_path" in result["message"]

    @pytest.mark.asyncio
    async def test_timeout(self, client):
        err = httpx.ConnectTimeout("timed out", request=httpx.Request("GET", "https://x.com"))
        with patch.object(client, "get_project", new_callable=AsyncMock, side_effect=err):
            result = await client.test_connection("PROJ")
        assert result["category"] == "timeout"

    @pytest.mark.asyncio
    async def test_generic_request_error(self, client):
        err = httpx.RequestError("boom", request=httpx.Request("GET", "https://x.com"))
        with patch.object(client, "get_project", new_callable=AsyncMock, side_effect=err):
            result = await client.test_connection("PROJ")
        assert result["category"] == "request_error"


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


class _FakeStreamResponse:
    """Minimal stand-in for an httpx streaming response."""

    def __init__(self, chunks: list[bytes], status_code: int = 200):
        self._chunks = chunks
        self.status_code = status_code
        self.request = httpx.Request("GET", "https://dss.example.com")

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            resp = httpx.Response(self.status_code, request=self.request)
            raise httpx.HTTPStatusError("error", request=self.request, response=resp)

    async def aiter_bytes(self, chunk_size: int = 65536):
        for c in self._chunks:
            yield c


class _FakeStreamCtx:
    def __init__(self, response: _FakeStreamResponse):
        self._response = response

    async def __aenter__(self) -> _FakeStreamResponse:
        return self._response

    async def __aexit__(self, *exc: object) -> bool:
        return False


class TestExportDataset:
    """`export_dataset()` previously had zero retry logic, unlike `_request()`."""

    @pytest.mark.asyncio
    async def test_success_returns_bytes(self, client):
        http_client = await client._ensure_client()
        resp = httpx.Response(200, content=b"a,b\n1,2\n", request=httpx.Request("GET", "https://x.com"))
        with patch.object(http_client, "get", new_callable=AsyncMock, return_value=resp):
            data = await client.export_dataset("PROJ", "orders")
        assert data == b"a,b\n1,2\n"

    @pytest.mark.asyncio
    async def test_retries_on_server_error_then_succeeds(self, client):
        http_client = await client._ensure_client()
        req = httpx.Request("GET", "https://x.com")
        fail_resp = httpx.Response(500, request=req)
        ok_resp = httpx.Response(200, content=b"ok", request=req)

        call_count = 0

        async def _fake_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return fail_resp if call_count == 1 else ok_resp

        with patch.object(http_client, "get", side_effect=_fake_get):
            data = await client.export_dataset("PROJ", "orders")
        assert data == b"ok"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_gives_up_after_max_retries(self, client):
        http_client = await client._ensure_client()
        resp = httpx.Response(500, request=httpx.Request("GET", "https://x.com"))
        with patch.object(http_client, "get", new_callable=AsyncMock, return_value=resp):
            with pytest.raises(httpx.HTTPStatusError):
                await client.export_dataset("PROJ", "orders")


class TestExportDatasetToFile:
    """Streaming export writes to a `.part` temp file and renames atomically,
    so a failed/interrupted download never leaves a corrupt file behind.
    """

    @pytest.mark.asyncio
    async def test_success_writes_final_file_and_removes_temp(self, client, tmp_path):
        http_client = await client._ensure_client()
        out = tmp_path / "orders.csv"
        response = _FakeStreamResponse([b"a,b\n", b"1,2\n"])
        with patch.object(http_client, "stream", return_value=_FakeStreamCtx(response)):
            result = await client.export_dataset_to_file("PROJ", "orders", str(out))

        assert out.exists()
        assert out.read_bytes() == b"a,b\n1,2\n"
        assert result["size_bytes"] == 8
        assert not out.with_name(out.name + ".part").exists()

    @pytest.mark.asyncio
    async def test_failure_leaves_no_file_at_output_path(self, client, tmp_path):
        http_client = await client._ensure_client()
        out = tmp_path / "orders.csv"
        response = _FakeStreamResponse([], status_code=500)
        with patch.object(http_client, "stream", return_value=_FakeStreamCtx(response)):
            with pytest.raises(httpx.HTTPStatusError):
                await client.export_dataset_to_file("PROJ", "orders", str(out))

        assert not out.exists()
        assert not out.with_name(out.name + ".part").exists()

    @pytest.mark.asyncio
    async def test_retries_on_server_error_then_succeeds(self, client, tmp_path):
        http_client = await client._ensure_client()
        out = tmp_path / "orders.csv"
        fail_response = _FakeStreamResponse([], status_code=500)
        ok_response = _FakeStreamResponse([b"a,b\n", b"1,2\n"])

        call_count = 0

        def _fake_stream(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return _FakeStreamCtx(fail_response if call_count == 1 else ok_response)

        with patch.object(http_client, "stream", side_effect=_fake_stream):
            result = await client.export_dataset_to_file("PROJ", "orders", str(out))

        assert out.read_bytes() == b"a,b\n1,2\n"
        assert result["size_bytes"] == 8
        assert call_count == 2


class TestClientCleanup:
    @pytest.mark.asyncio
    async def test_close_client(self, client):
        # Ensure close doesn't raise even if client was never used
        await client.close()
