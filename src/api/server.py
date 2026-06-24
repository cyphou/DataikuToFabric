"""REST API server — stdlib http.server based migration API."""

from __future__ import annotations

import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any
from urllib.parse import urlparse, parse_qs

from src.api.job_manager import JobManager, JobStatus
from src.core.observability import get_correlation_id, new_correlation_id, set_correlation_id
from src.core.registry import AssetRegistry


class MigrationAPIHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the migration API."""

    # Set by create_server()
    registry: AssetRegistry | None = None
    job_manager: JobManager | None = None
    auth_mode: str = "none"  # none | api_key | bearer
    auth_secret: str | None = None

    def _ensure_correlation_id(self) -> str:
        """Get correlation ID from request header or generate one."""
        incoming = self.headers.get("X-Correlation-ID")
        corr = incoming or get_correlation_id() or new_correlation_id("api")
        set_correlation_id(corr)
        return corr

    def _is_authorized(self) -> bool:
        """Return True if request auth passes based on configured mode."""
        mode = (self.auth_mode or "none").lower()
        if mode == "none":
            return True

        secret = self.auth_secret or ""
        if not secret:
            return False

        if mode == "api_key":
            provided = self.headers.get("X-API-Key", "")
            return provided == secret

        if mode == "bearer":
            auth_header = self.headers.get("Authorization", "")
            if not auth_header.lower().startswith("bearer "):
                return False
            token = auth_header.split(" ", 1)[1].strip()
            return token == secret

        return False

    def do_GET(self) -> None:
        self._ensure_correlation_id()
        if not self._is_authorized():
            self._send_json({"error": "Unauthorized"}, 401)
            return
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        params = parse_qs(parsed.query)

        routes: dict[str, Any] = {
            "/api/health": self._handle_health,
            "/api/status": self._handle_status,
            "/api/assets": self._handle_assets,
            "/api/jobs": self._handle_jobs,
        }

        # Check for parameterized routes
        if path.startswith("/api/assets/") and path.count("/") == 3:
            asset_id = path.split("/")[-1]
            self._handle_asset_detail(asset_id)
            return

        if path.startswith("/api/jobs/") and path.count("/") == 3:
            job_id = path.split("/")[-1]
            self._handle_job_detail(job_id)
            return

        handler = routes.get(path)
        if handler:
            handler(params)
        else:
            self._send_json({"error": "Not found"}, 404)

    def do_POST(self) -> None:
        self._ensure_correlation_id()
        if not self._is_authorized():
            self._send_json({"error": "Unauthorized"}, 401)
            return
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        if path == "/api/jobs":
            self._handle_create_job()
        elif path.startswith("/api/jobs/") and path.endswith("/cancel"):
            job_id = path.split("/")[-2]
            self._handle_cancel_job(job_id)
        else:
            self._send_json({"error": "Not found"}, 404)

    def _handle_health(self, params: dict) -> None:
        self._send_json({"status": "ok", "service": "dataiku-to-fabric-migration"})

    def _handle_status(self, params: dict) -> None:
        if not self.registry:
            self._send_json({"error": "Registry not initialized"}, 503)
            return
        stats = self.registry.get_statistics()
        self._send_json({
            "project_key": self.registry.project_key,
            "statistics": stats,
        })

    def _handle_assets(self, params: dict) -> None:
        if not self.registry:
            self._send_json({"error": "Registry not initialized"}, 503)
            return
        asset_type = params.get("type", [None])[0]
        state = params.get("state", [None])[0]
        name_contains = params.get("name_contains", [None])[0]
        page = max(1, int(params.get("page", [1])[0]))
        page_size = max(1, min(500, int(params.get("page_size", [50])[0])))

        assets = self.registry.get_all()

        if asset_type:
            assets = [a for a in assets if a.type.value == asset_type]
        if state:
            assets = [a for a in assets if a.state.value == state]
        if name_contains:
            assets = [a for a in assets if name_contains.lower() in a.name.lower()]

        total = len(assets)
        start = (page - 1) * page_size
        end = start + page_size
        assets = assets[start:end]

        self._send_json({
            "count": len(assets),
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
            "assets": [
                {
                    "id": a.id,
                    "name": a.name,
                    "type": a.type.value,
                    "state": a.state.value,
                    "error_count": len(a.errors),
                    "review_flag_count": len(a.review_flags),
                }
                for a in assets
            ],
        })

    def _handle_asset_detail(self, asset_id: str) -> None:
        if not self.registry:
            self._send_json({"error": "Registry not initialized"}, 503)
            return
        asset = self.registry.get_asset(asset_id)
        if not asset:
            self._send_json({"error": f"Asset '{asset_id}' not found"}, 404)
            return
        self._send_json({
            "id": asset.id,
            "name": asset.name,
            "type": asset.type.value,
            "state": asset.state.value,
            "metadata": asset.metadata,
            "dependencies": asset.dependencies,
            "target_fabric_asset": asset.target_fabric_asset,
            "errors": asset.errors,
            "review_flags": asset.review_flags,
        })

    def _handle_jobs(self, params: dict) -> None:
        if not self.job_manager:
            self._send_json({"error": "Job manager not initialized"}, 503)
            return
        status_filter = params.get("status", [None])[0]
        status = JobStatus(status_filter) if status_filter else None
        job_type = params.get("job_type", [None])[0]
        page = max(1, int(params.get("page", [1])[0]))
        page_size = max(1, min(500, int(params.get("page_size", [50])[0])))

        jobs = self.job_manager.list_jobs(status=status, job_type=job_type)
        total = len(jobs)
        start = (page - 1) * page_size
        end = start + page_size
        jobs = jobs[start:end]
        self._send_json({
            "count": len(jobs),
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
            "jobs": [j.to_dict() for j in jobs],
        })

    def _handle_job_detail(self, job_id: str) -> None:
        if not self.job_manager:
            self._send_json({"error": "Job manager not initialized"}, 503)
            return
        job = self.job_manager.get_job(job_id)
        if not job:
            self._send_json({"error": f"Job '{job_id}' not found"}, 404)
            return
        self._send_json(job.to_dict())

    def _handle_create_job(self) -> None:
        if not self.job_manager:
            self._send_json({"error": "Job manager not initialized"}, 503)
            return

        content_length = int(self.headers.get("Content-Length", 0))
        if content_length > 0:
            body = self.rfile.read(content_length)
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                self._send_json({"error": "Invalid JSON"}, 400)
                return
        else:
            data = {}

        job_type = data.get("job_type", "migration")
        parameters = data.get("parameters", {})
        webhook_url = data.get("webhook_url")
        job = self.job_manager.create_job(
            job_type=job_type,
            parameters=parameters,
            webhook_url=webhook_url,
        )
        self._send_json(job.to_dict(), 201)

    def _handle_cancel_job(self, job_id: str) -> None:
        if not self.job_manager:
            self._send_json({"error": "Job manager not initialized"}, 503)
            return
        if self.job_manager.cancel_job(job_id):
            job = self.job_manager.get_job(job_id)
            self._send_json(job.to_dict() if job else {"status": "cancelled"})
        else:
            self._send_json({"error": f"Cannot cancel job '{job_id}'"}, 400)

    def _send_json(self, data: dict[str, Any], status: int = 200) -> None:
        payload = dict(data)
        payload.setdefault("correlation_id", get_correlation_id())
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        if payload.get("correlation_id"):
            self.send_header("X-Correlation-ID", str(payload["correlation_id"]))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress default stderr logging."""
        pass


def create_server(
    host: str = "127.0.0.1",
    port: int = 8080,
    registry: AssetRegistry | None = None,
    job_manager: JobManager | None = None,
    auth_mode: str = "none",
    auth_secret: str | None = None,
) -> HTTPServer:
    """Create and configure the migration API server.

    Args:
        host: Bind address (default 127.0.0.1).
        port: Port number (default 8080).
        registry: Optional AssetRegistry for status endpoints.
        job_manager: Optional JobManager for job endpoints.

    Returns:
        Configured HTTPServer (call .serve_forever() to start).
    """
    MigrationAPIHandler.registry = registry
    MigrationAPIHandler.job_manager = job_manager or JobManager()
    MigrationAPIHandler.auth_mode = auth_mode
    MigrationAPIHandler.auth_secret = auth_secret

    server = HTTPServer((host, port), MigrationAPIHandler)
    return server
