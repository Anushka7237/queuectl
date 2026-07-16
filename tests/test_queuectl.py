from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from pathlib import Path

from typing import Iterator

import pytest

from queuectl.backoff import compute_backoff_delay, next_retry_time
from queuectl.models import Job, JobState, utc_now
from queuectl.queue import QueueService
from queuectl.settings import ConfigManager
from queuectl.storage import JobStorage
from queuectl.worker import WorkerManager


@pytest.fixture()
def temp_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    import shutil

    tmp = tempfile.mkdtemp()
    data_dir = Path(tmp)
    monkeypatch.setenv("QUEUECTL_DATA_DIR", str(data_dir))
    try:
        yield data_dir
    finally:
        db_path = data_dir / "queue.db"
        if db_path.exists():
            JobStorage(db_path).close()
        shutil.rmtree(data_dir, ignore_errors=True)


@pytest.fixture()
def service(temp_env: Path) -> Iterator[QueueService]:
    storage = JobStorage(temp_env / "queue.db")
    svc = QueueService(
        storage=storage,
        config=ConfigManager(temp_env / "config.json"),
    )
    yield svc
    storage.close()


def test_enqueue_and_persist(service: QueueService) -> None:
    job = service.enqueue({"id": "job1", "command": "echo hello"})
    assert job.state == JobState.PENDING
    assert job.max_retries == 3

    reloaded = service.storage.get_job("job1")
    assert reloaded is not None
    assert reloaded.command == "echo hello"


def test_duplicate_job_rejected(service: QueueService) -> None:
    service.enqueue({"id": "dup", "command": "echo one"})
    with pytest.raises(ValueError, match="already exists"):
        service.enqueue({"id": "dup", "command": "echo two"})


def test_list_and_status(service: QueueService) -> None:
    service.enqueue({"id": "a", "command": "echo a"})
    service.enqueue({"id": "b", "command": "echo b"})
    pending = service.list_jobs("pending")
    assert len(pending) == 2
    status = service.get_status()
    assert status["jobs"]["pending"] == 2
    assert status["total"] == 2


def test_config_set_and_apply(service: QueueService) -> None:
    service.config.set_value("max-retries", 5)
    service.config.set_value("backoff-base", 3)
    job = service.enqueue({"id": "cfg", "command": "echo cfg"})
    assert job.max_retries == 5
    assert service.config.backoff_base == 3


def test_backoff_formula() -> None:
    assert compute_backoff_delay(1, 2) == 2
    assert compute_backoff_delay(2, 2) == 4
    assert compute_backoff_delay(3, 2) == 8
    assert compute_backoff_delay(2, 3) == 9

    now = utc_now()
    retry_at = next_retry_time(2, 2, now=now)
    assert int((retry_at - now).total_seconds()) == 4


def test_successful_job_execution(service: QueueService) -> None:
    service.enqueue({"id": "ok", "command": "echo success"})
    manager = WorkerManager(storage=service.storage, config=service.config, count=1)
    thread = threading.Thread(target=manager.start)
    thread.start()

    deadline = time.time() + 10
    while time.time() < deadline:
        job = service.storage.get_job("ok")
        if job and job.state == JobState.COMPLETED:
            break
        time.sleep(0.1)
    else:
        pytest.fail("Job did not complete in time")

    manager.request_stop()
    thread.join(timeout=5)
    assert job.exit_code == 0


def test_failed_job_moves_to_dlq(service: QueueService) -> None:
    service.config.set_value("max-retries", 2)
    service.config.set_value("backoff-base", 1)
    service.enqueue({"id": "fail", "command": "exit 1", "max_retries": 2})

    manager = WorkerManager(
        storage=service.storage,
        config=service.config,
        count=1,
        poll_interval=0.1,
    )
    thread = threading.Thread(target=manager.start)
    thread.start()

    deadline = time.time() + 20
    while time.time() < deadline:
        job = service.storage.get_job("fail")
        if job and job.state == JobState.DEAD:
            break
        time.sleep(0.2)
    else:
        manager.request_stop()
        thread.join(timeout=5)
        pytest.fail("Job did not reach DLQ in time")

    manager.request_stop()
    thread.join(timeout=5)
    assert job.attempts == 3
    assert len(service.list_dlq()) == 1


def test_dlq_retry(service: QueueService) -> None:
    job = Job(id="dead1", command="exit 1", state=JobState.DEAD, attempts=3)
    service.storage.add_job(job)
    retried = service.retry_dlq_job("dead1")
    assert retried.state == JobState.PENDING
    assert retried.attempts == 0


def test_claim_prevents_duplicate_processing(service: QueueService) -> None:
    service.enqueue({"id": "race", "command": "sleep 1"})
    claimed = []
    lock = threading.Lock()

    def claim(worker_id: str) -> None:
        job = service.storage.claim_next_job(worker_id)
        if job:
            with lock:
                claimed.append(worker_id)

    threads = [threading.Thread(target=claim, args=(f"w{i}",)) for i in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(claimed) == 1


def test_persistence_survives_restart(temp_env: Path) -> None:
    db_path = temp_env / "queue.db"
    config_path = temp_env / "config.json"

    first_storage = JobStorage(db_path)
    first = QueueService(storage=first_storage, config=ConfigManager(config_path))
    first.enqueue({"id": "persist", "command": "echo persist"})
    first_storage.close()

    second_storage = JobStorage(db_path)
    second = QueueService(storage=second_storage, config=ConfigManager(config_path))
    job = second.storage.get_job("persist")
    second_storage.close()
    assert job is not None
    assert job.command == "echo persist"
