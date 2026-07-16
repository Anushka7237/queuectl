from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class JobState(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


@dataclass
class Job:
    id: str
    command: str
    state: JobState = JobState.PENDING
    attempts: int = 0
    max_retries: int = 3
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
    next_retry_at: datetime | None = None
    locked_by: str | None = None
    locked_at: datetime | None = None
    last_error: str | None = None
    exit_code: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "command": self.command,
            "state": self.state.value,
            "attempts": self.attempts,
            "max_retries": self.max_retries,
            "created_at": isoformat(self.created_at),
            "updated_at": isoformat(self.updated_at),
            "next_retry_at": isoformat(self.next_retry_at) if self.next_retry_at else None,
            "locked_by": self.locked_by,
            "locked_at": isoformat(self.locked_at) if self.locked_at else None,
            "last_error": self.last_error,
            "exit_code": self.exit_code,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Job:
        return cls(
            id=data["id"],
            command=data["command"],
            state=JobState(data["state"]),
            attempts=int(data.get("attempts", 0)),
            max_retries=int(data.get("max_retries", 3)),
            created_at=parse_iso(data["created_at"]),
            updated_at=parse_iso(data["updated_at"]),
            next_retry_at=parse_iso(data["next_retry_at"]) if data.get("next_retry_at") else None,
            locked_by=data.get("locked_by"),
            locked_at=parse_iso(data["locked_at"]) if data.get("locked_at") else None,
            last_error=data.get("last_error"),
            exit_code=data.get("exit_code"),
        )

    @classmethod
    def from_enqueue_payload(cls, payload: dict[str, Any], default_max_retries: int) -> Job:
        job_id = payload.get("id")
        command = payload.get("command")
        if not job_id or not isinstance(job_id, str):
            raise ValueError("Job payload must include a string 'id'")
        if not command or not isinstance(command, str):
            raise ValueError("Job payload must include a string 'command'")

        now = utc_now()
        return cls(
            id=job_id,
            command=command,
            state=JobState.PENDING,
            attempts=0,
            max_retries=int(payload.get("max_retries", default_max_retries)),
            created_at=now,
            updated_at=now,
        )
