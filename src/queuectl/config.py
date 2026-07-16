from __future__ import annotations

import os
from pathlib import Path

DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 2

CONFIG_KEYS = {
    "max-retries": "max_retries",
    "backoff-base": "backoff_base",
}


def get_data_dir() -> Path:
    env = os.environ.get("QUEUECTL_DATA_DIR")
    if env:
        path = Path(env)
    else:
        path = Path.home() / ".queuectl"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_db_path() -> Path:
    return get_data_dir() / "queue.db"


def get_config_path() -> Path:
    return get_data_dir() / "config.json"


def get_worker_pid_path() -> Path:
    return get_data_dir() / "worker.pid"


def get_worker_stop_path() -> Path:
    return get_data_dir() / "worker.stop"
