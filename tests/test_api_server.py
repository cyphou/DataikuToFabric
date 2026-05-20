"""Tests for Phase 24 — REST API Server."""

from __future__ import annotations

import json
import threading
import time
import urllib.request
import urllib.error

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
