"""Job manager — in-memory async job store for migration tasks."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


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
        }


class JobManager:
    """In-memory job store for managing migration tasks."""

    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}

    def create_job(self, job_type: str, parameters: dict[str, Any] | None = None) -> Job:
        """Create a new pending job."""
        job = Job(
            job_id=str(uuid.uuid4()),
            job_type=job_type,
            status=JobStatus.PENDING,
            created_at=datetime.now(timezone.utc).isoformat(),
            parameters=parameters or {},
        )
        self._jobs[job.job_id] = job
        return job

    def get_job(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    def list_jobs(self, status: JobStatus | None = None) -> list[Job]:
        if status is None:
            return list(self._jobs.values())
        return [j for j in self._jobs.values() if j.status == status]

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
        return True

    def fail_job(self, job_id: str, error: str) -> bool:
        job = self._jobs.get(job_id)
        if not job or job.status != JobStatus.RUNNING:
            return False
        job.status = JobStatus.FAILED
        job.completed_at = datetime.now(timezone.utc).isoformat()
        job.error = error
        return True

    def cancel_job(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if not job or job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
            return False
        job.status = JobStatus.CANCELLED
        job.completed_at = datetime.now(timezone.utc).isoformat()
        return True

    def cleanup_completed(self) -> int:
        """Remove completed/failed/cancelled jobs. Returns count removed."""
        to_remove = [
            jid for jid, j in self._jobs.items()
            if j.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)
        ]
        for jid in to_remove:
            del self._jobs[jid]
        return len(to_remove)
