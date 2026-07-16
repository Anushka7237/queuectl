from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator

from queuectl.config import get_db_path
from queuectl.models import Job, JobState, isoformat, parse_iso, utc_now


class JobStorage:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or get_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    command TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 3,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    next_retry_at TEXT,
                    locked_by TEXT,
                    locked_at TEXT,
                    last_error TEXT,
                    exit_code INTEGER
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_jobs_next_retry ON jobs(next_retry_at)"
            )
            conn.commit()

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> Job:
        return Job.from_dict(dict(row))

    def add_job(self, job: Job) -> Job:
        with self.connect() as conn:
            existing = conn.execute(
                "SELECT id FROM jobs WHERE id = ?", (job.id,)
            ).fetchone()
            if existing:
                raise ValueError(f"Job with id '{job.id}' already exists")

            conn.execute(
                """
                INSERT INTO jobs (
                    id, command, state, attempts, max_retries,
                    created_at, updated_at, next_retry_at,
                    locked_by, locked_at, last_error, exit_code
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.id,
                    job.command,
                    job.state.value,
                    job.attempts,
                    job.max_retries,
                    isoformat(job.created_at),
                    isoformat(job.updated_at),
                    isoformat(job.next_retry_at) if job.next_retry_at else None,
                    job.locked_by,
                    isoformat(job.locked_at) if job.locked_at else None,
                    job.last_error,
                    job.exit_code,
                ),
            )
        return job

    def get_job(self, job_id: str) -> Job | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._row_to_job(row) if row else None

    def list_jobs(self, state: JobState | None = None) -> list[Job]:
        with self.connect() as conn:
            if state:
                rows = conn.execute(
                    "SELECT * FROM jobs WHERE state = ? ORDER BY created_at ASC",
                    (state.value,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM jobs ORDER BY created_at ASC"
                ).fetchall()
        return [self._row_to_job(row) for row in rows]

    def count_by_state(self) -> dict[str, int]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT state, COUNT(*) AS count FROM jobs GROUP BY state"
            ).fetchall()
        counts = {state.value: 0 for state in JobState}
        for row in rows:
            counts[row["state"]] = row["count"]
        return counts

    def update_job(self, job: Job) -> Job:
        job.updated_at = utc_now()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE jobs SET
                    command = ?, state = ?, attempts = ?, max_retries = ?,
                    updated_at = ?, next_retry_at = ?, locked_by = ?,
                    locked_at = ?, last_error = ?, exit_code = ?
                WHERE id = ?
                """,
                (
                    job.command,
                    job.state.value,
                    job.attempts,
                    job.max_retries,
                    isoformat(job.updated_at),
                    isoformat(job.next_retry_at) if job.next_retry_at else None,
                    job.locked_by,
                    isoformat(job.locked_at) if job.locked_at else None,
                    job.last_error,
                    job.exit_code,
                    job.id,
                ),
            )
        return job

    def delete_job(self, job_id: str) -> bool:
        with self.connect() as conn:
            cursor = conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        return cursor.rowcount > 0

    def claim_next_job(self, worker_id: str, now: datetime | None = None) -> Job | None:
        """Atomically claim the next runnable job to prevent duplicate processing."""
        now = now or utc_now()
        now_iso = isoformat(now)

        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM jobs
                WHERE (
                    state = ?
                    OR (state = ? AND (next_retry_at IS NULL OR next_retry_at <= ?))
                )
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (JobState.PENDING.value, JobState.FAILED.value, now_iso),
            ).fetchone()

            if row is None:
                return None

            updated = conn.execute(
                """
                UPDATE jobs SET
                    state = ?, locked_by = ?, locked_at = ?, updated_at = ?
                WHERE id = ? AND state IN (?, ?)
                  AND (next_retry_at IS NULL OR next_retry_at <= ?)
                """,
                (
                    JobState.PROCESSING.value,
                    worker_id,
                    now_iso,
                    now_iso,
                    row["id"],
                    JobState.PENDING.value,
                    JobState.FAILED.value,
                    now_iso,
                ),
            )
            if updated.rowcount != 1:
                return None

            claimed = conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (row["id"],)
            ).fetchone()

        return self._row_to_job(claimed) if claimed else None

    def close(self) -> None:
        """Release SQLite WAL locks (important on Windows during test cleanup)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def release_stale_locks(self, stale_before: datetime) -> int:
        stale_iso = isoformat(stale_before)
        with self.connect() as conn:
            cursor = conn.execute(
                """
                UPDATE jobs SET
                    state = ?, locked_by = NULL, locked_at = NULL, updated_at = ?
                WHERE state = ? AND locked_at IS NOT NULL AND locked_at < ?
                """,
                (
                    JobState.PENDING.value,
                    isoformat(utc_now()),
                    JobState.PROCESSING.value,
                    stale_iso,
                ),
            )
        return cursor.rowcount
