from __future__ import annotations

from datetime import datetime, timedelta

from queuectl.models import utc_now


def compute_backoff_delay(attempts: int, base: int) -> int:
    """Exponential backoff: delay = base ^ attempts (seconds)."""
    if attempts <= 0:
        return 0
    return int(base**attempts)


def next_retry_time(attempts: int, base: int, now: datetime | None = None) -> datetime:
    now = now or utc_now()
    delay = compute_backoff_delay(attempts, base)
    return now + timedelta(seconds=delay)
