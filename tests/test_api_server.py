"""Tests for Phase 24 — REST API Server."""

from __future__ import annotations

import json
import threading
import time
import urllib.request
import urllib.error
from unittest.mock import patch

import pytest

from src.api.job_manager import Job, JobManager, JobStatus
from src.api.server import MigrationAPIHandler, create_server
from src.core.registry import AssetRegistry
from src.models.asset import Asset, AssetType, MigrationState


# ── JobManager ────────────────────────────────────────────────

class TestJobManager:
    def test_create_job(self):
        jm = JobManager()
        job = jm.create_job(job_type="migration")
        assert job.status == JobStatus.PENDING
        assert job.job_type == "migration"

    def test_get_job(self):
        jm = JobManager()
        job = jm.create_job(job_type="test")
        fetched = jm.get_job(job.job_id)
        assert fetched is not None
        assert fetched.job_id == job.job_id

    def test_get_nonexistent_job(self):
        jm = JobManager()
        assert jm.get_job("nonexistent") is None

    def test_list_jobs(self):
        jm = JobManager()
        jm.create_job(job_type="a")
        jm.create_job(job_type="b")
        assert len(jm.list_jobs()) == 2

    def test_list_jobs_filter_status(self):
        jm = JobManager()
        j1 = jm.create_job(job_type="a")
        j2 = jm.create_job(job_type="b")
        jm.start_job(j1.job_id)
        running = jm.list_jobs(status=JobStatus.RUNNING)
        assert len(running) == 1

    def test_start_job(self):
        jm = JobManager()
        job = jm.create_job(job_type="test")
        jm.start_job(job.job_id)
        assert jm.get_job(job.job_id).status == JobStatus.RUNNING

    def test_complete_job(self):
        jm = JobManager()
        job = jm.create_job(job_type="test")
        jm.start_job(job.job_id)
        jm.complete_job(job.job_id, result={"data": "ok"})
        completed = jm.get_job(job.job_id)
        assert completed.status == JobStatus.COMPLETED

    def test_fail_job(self):
        jm = JobManager()
        job = jm.create_job(job_type="test")
        jm.start_job(job.job_id)
        jm.fail_job(job.job_id, error="boom")
        assert jm.get_job(job.job_id).status == JobStatus.FAILED

    def test_cancel_job(self):
        jm = JobManager()
        job = jm.create_job(job_type="test")
        result = jm.cancel_job(job.job_id)
        assert result is True
        assert jm.get_job(job.job_id).status == JobStatus.CANCELLED

    def test_cancel_completed_job(self):
        jm = JobManager()
        job = jm.create_job(job_type="test")
        jm.start_job(job.job_id)
        jm.complete_job(job.job_id)
        result = jm.cancel_job(job.job_id)
        assert result is False

    def test_update_progress(self):
        jm = JobManager()
        job = jm.create_job(job_type="test")
        jm.start_job(job.job_id)
        jm.update_progress(job.job_id, 50.0)
        updated = jm.get_job(job.job_id)
        assert updated.progress == 50.0

    def test_job_to_dict(self):
        jm = JobManager()
        job = jm.create_job(job_type="test", parameters={"key": "val"})
        d = job.to_dict()
        assert d["job_type"] == "test"
        assert d["parameters"]["key"] == "val"

    def test_cleanup_completed(self):
        jm = JobManager()
        j1 = jm.create_job(job_type="a")
        j2 = jm.create_job(job_type="b")
        jm.start_job(j1.job_id)
        jm.complete_job(j1.job_id)
        jm.cleanup_completed()
        assert jm.get_job(j1.job_id) is None
        assert jm.get_job(j2.job_id) is not None

    def test_webhook_called_on_complete(self):
        jm = JobManager()
        job = jm.create_job(job_type="test", webhook_url="https://example.test/hook")
        jm.start_job(job.job_id)

        with patch("urllib.request.urlopen"):
            ok = jm.complete_job(job.job_id, result={"x": 1})

        assert ok is True
        updated = jm.get_job(job.job_id)
        assert updated.webhook_notified is True

    def test_webhook_error_captured(self):
        jm = JobManager()
        job = jm.create_job(job_type="test", webhook_url="https://example.test/hook")
        jm.start_job(job.job_id)

        with patch("urllib.request.urlopen", side_effect=RuntimeError("boom")):
            jm.fail_job(job.job_id, "failed")

        updated = jm.get_job(job.job_id)
        assert updated.webhook_notified is False
        assert "boom" in updated.webhook_error

    def test_list_jobs_filter_job_type(self):
        jm = JobManager()
        jm.create_job(job_type="migration")
        jm.create_job(job_type="validation")
        filtered = jm.list_jobs(job_type="validation")
        assert len(filtered) == 1
        assert filtered[0].job_type == "validation"


# ── API Server ────────────────────────────────────────────────

@pytest.fixture
def api_server(tmp_path):
    """Create and start a test server on a random port."""
    reg = AssetRegistry(project_key="API_TEST", registry_path=tmp_path / "reg.json")
    reg.add_asset(Asset(
        id="ds_test", name="test_dataset", type=AssetType.DATASET,
        source_project="API_TEST", state=MigrationState.DISCOVERED,
        metadata={"schema": [{"name": "id", "type": "int"}]},
    ))

    jm = JobManager()
    server = create_server(host="127.0.0.1", port=0, registry=reg, job_manager=jm)
    port = server.server_address[1]

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    time.sleep(0.1)

    yield f"http://127.0.0.1:{port}", server, reg, jm

    server.shutdown()


def _get(url: str) -> tuple[int, dict]:
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def _get_with_headers(url: str, headers: dict[str, str]) -> tuple[int, dict]:
    req = urllib.request.Request(url)
    for k, v in headers.items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def _post(url: str, data: dict | None = None) -> tuple[int, dict]:
    body = json.dumps(data or {}).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


class TestAPIServer:
    def test_health(self, api_server):
        base, *_ = api_server
        status, data = _get(f"{base}/api/health")
        assert status == 200
        assert data["status"] == "ok"
        assert "correlation_id" in data

    def test_health_uses_incoming_correlation_id(self, api_server):
        base, *_ = api_server
        status, data = _get_with_headers(
            f"{base}/api/health", {"X-Correlation-ID": "test-corr-id-123"}
        )
        assert status == 200
        assert data["correlation_id"] == "test-corr-id-123"

    def test_status(self, api_server):
        base, *_ = api_server
        status, data = _get(f"{base}/api/status")
        assert status == 200
        assert data["project_key"] == "API_TEST"

    def test_list_assets(self, api_server):
        base, *_ = api_server
        status, data = _get(f"{base}/api/assets")
        assert status == 200
        assert data["count"] >= 1

    def test_get_asset_detail(self, api_server):
        base, *_ = api_server
        status, data = _get(f"{base}/api/assets/ds_test")
        assert status == 200
        assert data["name"] == "test_dataset"

    def test_get_asset_not_found(self, api_server):
        base, *_ = api_server
        status, data = _get(f"{base}/api/assets/nonexistent")
        assert status == 404

    def test_create_job(self, api_server):
        base, *_ = api_server
        status, data = _post(f"{base}/api/jobs", {"job_type": "test"})
        assert status == 201
        assert "job_id" in data

    def test_list_jobs(self, api_server):
        base, *_ = api_server
        _post(f"{base}/api/jobs", {"job_type": "a"})
        status, data = _get(f"{base}/api/jobs")
        assert status == 200
        assert data["count"] >= 1

    def test_jobs_pagination_and_filter(self, api_server):
        base, *_ = api_server
        _post(f"{base}/api/jobs", {"job_type": "migration"})
        _post(f"{base}/api/jobs", {"job_type": "validation"})
        _post(f"{base}/api/jobs", {"job_type": "validation"})

        status, data = _get(f"{base}/api/jobs?job_type=validation&page=1&page_size=1")
        assert status == 200
        assert data["page"] == 1
        assert data["page_size"] == 1
        assert data["total"] >= 2
        assert data["count"] == 1
        assert data["jobs"][0]["job_type"] == "validation"

    def test_assets_pagination_and_name_filter(self, api_server):
        base, *_ = api_server
        status, data = _get(f"{base}/api/assets?name_contains=test&page=1&page_size=1")
        assert status == 200
        assert data["page"] == 1
        assert data["page_size"] == 1
        assert data["count"] <= 1
        assert data["total"] >= 1

    def test_cancel_job(self, api_server):
        base, *_ = api_server
        _, create_data = _post(f"{base}/api/jobs", {"job_type": "cancel_test"})
        job_id = create_data["job_id"]
        status, data = _post(f"{base}/api/jobs/{job_id}/cancel")
        assert status == 200

    def test_not_found(self, api_server):
        base, *_ = api_server
        status, data = _get(f"{base}/api/nonexistent")
        assert status == 404

    def test_auth_api_key_mode(self, tmp_path):
        reg = AssetRegistry(project_key="API_TEST", registry_path=tmp_path / "reg.json")
        server = create_server(
            host="127.0.0.1",
            port=0,
            registry=reg,
            job_manager=JobManager(),
            auth_mode="api_key",
            auth_secret="secret123",
        )
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        time.sleep(0.1)

        try:
            status, _ = _get(f"http://127.0.0.1:{port}/api/health")
            assert status == 401

            status, data = _get_with_headers(
                f"http://127.0.0.1:{port}/api/health",
                {"X-API-Key": "secret123"},
            )
            assert status == 200
            assert data["status"] == "ok"
        finally:
            server.shutdown()

    def test_auth_bearer_mode(self, tmp_path):
        reg = AssetRegistry(project_key="API_TEST", registry_path=tmp_path / "reg.json")
        server = create_server(
            host="127.0.0.1",
            port=0,
            registry=reg,
            job_manager=JobManager(),
            auth_mode="bearer",
            auth_secret="token-xyz",
        )
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        time.sleep(0.1)

        try:
            status, _ = _get(f"http://127.0.0.1:{port}/api/health")
            assert status == 401

            status, data = _get_with_headers(
                f"http://127.0.0.1:{port}/api/health",
                {"Authorization": "Bearer token-xyz"},
            )
            assert status == 200
            assert data["status"] == "ok"
        finally:
            server.shutdown()
