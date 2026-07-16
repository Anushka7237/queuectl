from __future__ import annotations

from queuectl.config import get_worker_pid_path
from queuectl.models import Job, JobState
from queuectl.settings import ConfigManager
from queuectl.storage import JobStorage


class QueueService:
    def __init__(
        self,
        storage: JobStorage | None = None,
        config: ConfigManager | None = None,
    ) -> None:
        self.storage = storage or JobStorage()
        self.config = config or ConfigManager()

    def enqueue(self, payload: dict) -> Job:
        job = Job.from_enqueue_payload(payload, self.config.max_retries)
        return self.storage.add_job(job)

    def list_jobs(self, state: str | None = None) -> list[Job]:
        parsed = JobState(state) if state else None
        return self.storage.list_jobs(parsed)

    def get_status(self) -> dict:
        counts = self.storage.count_by_state()
        config = self.config.as_display_dict()
        worker_pid = None
        pid_path = get_worker_pid_path()
        if pid_path.exists():
            try:
                worker_pid = int(pid_path.read_text(encoding="utf-8").strip())
            except ValueError:
                worker_pid = None
        return {
            "jobs": counts,
            "total": sum(counts.values()),
            "config": config,
            "active_workers": 1 if worker_pid else 0,
            "worker_pid": worker_pid,
        }

    def list_dlq(self) -> list[Job]:
        return self.storage.list_jobs(JobState.DEAD)

    def retry_dlq_job(self, job_id: str) -> Job:
        job = self.storage.get_job(job_id)
        if job is None:
            raise ValueError(f"Job '{job_id}' not found")
        if job.state != JobState.DEAD:
            raise ValueError(f"Job '{job_id}' is not in the dead letter queue")

        job.state = JobState.PENDING
        job.attempts = 0
        job.next_retry_at = None
        job.locked_by = None
        job.locked_at = None
        job.last_error = None
        job.exit_code = None
        return self.storage.update_job(job)
