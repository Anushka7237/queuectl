from __future__ import annotations

import json
from pathlib import Path

from queuectl.config import (
    DEFAULT_BACKOFF_BASE,
    DEFAULT_MAX_RETRIES,
    get_config_path,
)


class ConfigManager:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or get_config_path()
        self._cache: dict[str, int] | None = None

    def _load_raw(self) -> dict[str, int]:
        if not self.path.exists():
            return {}
        with self.path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            raise ValueError("Config file must contain a JSON object")
        return {str(k): int(v) for k, v in data.items()}

    def load(self) -> dict[str, int]:
        if self._cache is None:
            self._cache = self._load_raw()
        return dict(self._cache)

    def save(self, values: dict[str, int]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as fh:
            json.dump(values, fh, indent=2)
        self._cache = dict(values)

    def get(self, key: str, default: int) -> int:
        return self.load().get(key, default)

    @property
    def max_retries(self) -> int:
        return self.get("max_retries", DEFAULT_MAX_RETRIES)

    @property
    def backoff_base(self) -> int:
        return self.get("backoff_base", DEFAULT_BACKOFF_BASE)

    def set_value(self, cli_key: str, value: int) -> None:
        from queuectl.config import CONFIG_KEYS

        internal = CONFIG_KEYS.get(cli_key)
        if internal is None:
            raise ValueError(f"Unknown config key: {cli_key}")
        if value < 0:
            raise ValueError(f"{cli_key} must be non-negative")
        if cli_key == "backoff-base" and value < 1:
            raise ValueError("backoff-base must be at least 1")

        current = self.load()
        current[internal] = value
        self.save(current)

    def as_display_dict(self) -> dict[str, int]:
        loaded = self.load()
        return {
            "max-retries": loaded.get("max_retries", DEFAULT_MAX_RETRIES),
            "backoff-base": loaded.get("backoff_base", DEFAULT_BACKOFF_BASE),
        }
