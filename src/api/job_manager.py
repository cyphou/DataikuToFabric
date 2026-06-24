"""Job manager — in-memory async job store for migration tasks."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
import urllib.request


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Job:
    """Represents an async migration job."""
    job_id: str = ""
    job_type: str = ""
    status: JobStatus = JobStatus.PENDING
    created_at: str = ""
    started_at: str = ""
    completed_at: str = ""
    progress: float = 0.0
    result: dict[str, Any] = field(default_factory=dict)
    error: str = ""
    parameters: dict[str, Any] = field(default_factory=dict)
    webhook_url: str = ""
    webhook_notified: bool = False
    webhook_error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "job_type": self.job_type,
            "status": self.status.value,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "progress": self.progress,
            "result": self.result,
            "error": self.error,
            "parameters": self.parameters,
            "webhook_url": self.webhook_url,
            "webhook_notified": self.webhook_notified,
            "webhook_error": self.webhook_error,
        }


class JobManager:
    """In-memory job store for managing migration tasks."""

    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}

    def create_job(
        self,
        job_type: str,
        parameters: dict[str, Any] | None = None,
        webhook_url: str | None = None,
    ) -> Job:
        """Create a new pending job."""
        job = Job(
            job_id=str(uuid.uuid4()),
            job_type=job_type,
            status=JobStatus.PENDING,
            created_at=datetime.now(timezone.utc).isoformat(),
            parameters=parameters or {},
            webhook_url=webhook_url or "",
        )
        self._jobs[job.job_id] = job
        return job

    def get_job(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    def list_jobs(
        self,
        status: JobStatus | None = None,
        *,
        job_type: str | None = None,
    ) -> list[Job]:
        jobs = list(self._jobs.values())
        if status is not None:
            jobs = [j for j in jobs if j.status == status]
        if job_type:
            jobs = [j for j in jobs if j.job_type == job_type]
        return jobs

    def start_job(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if not job or job.status != JobStatus.PENDING:
            return False
        job.status = JobStatus.RUNNING
        job.started_at = datetime.now(timezone.utc).isoformat()
        return True

    def update_progress(self, job_id: str, progress: float) -> bool:
        job = self._jobs.get(job_id)
        if not job or job.status != JobStatus.RUNNING:
            return False
        job.progress = min(100.0, max(0.0, progress))
        return True

    def complete_job(self, job_id: str, result: dict[str, Any] | None = None) -> bool:
        job = self._jobs.get(job_id)
        if not job or job.status != JobStatus.RUNNING:
            return False
        job.status = JobStatus.COMPLETED
        job.completed_at = datetime.now(timezone.utc).isoformat()
        job.progress = 100.0
        job.result = result or {}
        self._notify_webhook(job)
        return True

    def fail_job(self, job_id: str, error: str) -> bool:
        job = self._jobs.get(job_id)
        if not job or job.status != JobStatus.RUNNING:
            return False
        job.status = JobStatus.FAILED
        job.completed_at = datetime.now(timezone.utc).isoformat()
        job.error = error
        self._notify_webhook(job)
        return True

    def cancel_job(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if not job or job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
            return False
        job.status = JobStatus.CANCELLED
        job.completed_at = datetime.now(timezone.utc).isoformat()
        self._notify_webhook(job)
        return True

    def _notify_webhook(self, job: Job) -> None:
        """Best-effort webhook callback on terminal job states."""
        if not job.webhook_url:
            return

        payload = job.to_dict()
        try:
            import json

            body = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                job.webhook_url,
                data=body,
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=3):
                pass
            job.webhook_notified = True
            job.webhook_error = ""
        except Exception as exc:
            job.webhook_notified = False
            job.webhook_error = str(exc)

    def cleanup_completed(self) -> int:
        """Remove completed/failed/cancelled jobs. Returns count removed."""
        to_remove = [
            jid for jid, j in self._jobs.items()
            if j.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)
        ]
        for jid in to_remove:
            del self._jobs[jid]
        return len(to_remove)
