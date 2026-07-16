from __future__ import annotations

import os
import signal
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import timedelta

from queuectl.backoff import next_retry_time
from queuectl.config import get_worker_pid_path, get_worker_stop_path
from queuectl.models import Job, JobState, utc_now
from queuectl.settings import ConfigManager
from queuectl.storage import JobStorage


@dataclass
class WorkerManager:
    storage: JobStorage
    config: ConfigManager
    count: int = 1
    poll_interval: float = 0.5
    _stop_event: threading.Event = field(default_factory=threading.Event, init=False)
    _threads: list[threading.Thread] = field(default_factory=list, init=False)
    _current_jobs: dict[str, Job | None] = field(default_factory=dict, init=False)

    def start(self) -> None:
        self._write_pid_file()
        self._clear_stop_file()
        self._stop_event.clear()

        for index in range(self.count):
            worker_id = f"worker-{index + 1}-{uuid.uuid4().hex[:8]}"
            self._current_jobs[worker_id] = None
            thread = threading.Thread(
                target=self._run_loop,
                args=(worker_id,),
                name=worker_id,
                daemon=False,
            )
            self._threads.append(thread)
            thread.start()

        for thread in self._threads:
            thread.join()

        self._cleanup_pid_file()

    def request_stop(self) -> None:
        get_worker_stop_path().write_text("stop", encoding="utf-8")
        self._stop_event.set()

    def _write_pid_file(self) -> None:
        get_worker_pid_path().write_text(str(os.getpid()), encoding="utf-8")

    def _cleanup_pid_file(self) -> None:
        pid_path = get_worker_pid_path()
        if pid_path.exists():
            pid_path.unlink()

    def _clear_stop_file(self) -> None:
        stop_path = get_worker_stop_path()
        if stop_path.exists():
            stop_path.unlink()

    def _should_stop(self) -> bool:
        if self._stop_event.is_set():
            return True
        return get_worker_stop_path().exists()

    def _run_loop(self, worker_id: str) -> None:
        while not self._should_stop():
            self.storage.release_stale_locks(utc_now() - timedelta(minutes=30))
            job = self.storage.claim_next_job(worker_id)
            if job is None:
                time.sleep(self.poll_interval)
                continue

            self._current_jobs[worker_id] = job
            try:
                self._process_job(job)
            finally:
                self._current_jobs[worker_id] = None

    def _process_job(self, job: Job) -> None:
        job.attempts += 1
        job.updated_at = utc_now()

        try:
            result = subprocess.run(
                job.command,
                shell=True,
                capture_output=True,
                text=True,
            )
            exit_code = result.returncode
            stderr = (result.stderr or "").strip()
        except Exception as exc:  # noqa: BLE001 - record execution errors on the job
            exit_code = 127
            stderr = str(exc)

        job.exit_code = exit_code
        job.locked_by = None
        job.locked_at = None

        if exit_code == 0:
            job.state = JobState.COMPLETED
            job.last_error = None
            job.next_retry_at = None
        else:
            job.last_error = stderr or f"Command exited with code {exit_code}"
            if job.attempts > job.max_retries:
                job.state = JobState.DEAD
                job.next_retry_at = None
            else:
                job.state = JobState.FAILED
                job.next_retry_at = next_retry_time(
                    job.attempts,
                    self.config.backoff_base,
                )

        self.storage.update_job(job)


def install_signal_handlers(manager: WorkerManager) -> None:
    def _handler(signum: int, frame: object) -> None:  # noqa: ARG001
        manager.request_stop()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _handler)


def read_worker_pid() -> int | None:
    pid_path = get_worker_pid_path()
    if not pid_path.exists():
        return None
    try:
        return int(pid_path.read_text(encoding="utf-8").strip())
    except ValueError:
        return None
