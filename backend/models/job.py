from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(ts: datetime | None = None) -> str:
    value = ts or utcnow()
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class JobType(str, Enum):
    IMAGE_PROCESSING = "image_processing"
    REPORT_GENERATION = "report_generation"
    DATA_TRANSFORMATION = "data_transformation"


class JobStatus(str, Enum):
    QUEUED = "queued"
    ASSIGNED = "assigned"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILURE = "failure"
    RETRY = "retry"
    DEAD_LETTER = "dead-letter"


class WorkerState(str, Enum):
    STARTING = "STARTING"
    CONNECTING = "CONNECTING"
    READY = "READY"
    IDLE = "IDLE"
    PROCESSING = "PROCESSING"
    DRAINING = "DRAINING"
    DEAD = "DEAD"
    RECOVERING = "RECOVERING"


class JobCreate(BaseModel):
    type: JobType
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None


class Job(BaseModel):
    job_id: str
    type: JobType
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str
    attempt: int = 1
    status: JobStatus = JobStatus.QUEUED
    created_at: str
    updated_at: str
    assigned_worker: str | None = None
    first_worker: str | None = None
    recovery_worker: str | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    failure_reason: str | None = None
    skipped_duplicate: bool = False
    side_effect_committed: bool = False
    redelivered: bool = False
    inflight_failure_at: str | None = None
    recovered_at: str | None = None
    recovery_ms: float | None = None
    timeline: list[dict[str, Any]] = Field(default_factory=list)

    @classmethod
    def new(cls, data: JobCreate) -> "Job":
        now = iso()
        job_id = f"job_{uuid4().hex[:10]}"
        idem = data.idempotency_key or f"idem_{uuid4().hex[:12]}"
        job = cls(
            job_id=job_id,
            type=data.type,
            payload=data.payload,
            idempotency_key=idem,
            created_at=now,
            updated_at=now,
        )
        job.add_event("job_queued", status=JobStatus.QUEUED.value)
        return job

    def add_event(self, event: str, **extra: Any) -> dict[str, Any]:
        entry = {
            "event": event,
            "timestamp": iso(),
            "status": self.status.value if isinstance(self.status, JobStatus) else self.status,
            "attempt": self.attempt,
            "worker_id": self.assigned_worker,
            **extra,
        }
        self.timeline.append(entry)
        self.updated_at = entry["timestamp"]
        return entry
