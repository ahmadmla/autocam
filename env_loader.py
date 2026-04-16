"""Minimal repo-local .env loader.

Values from the real process environment win over .env values. Lines may be either
KEY=value or export KEY=value. This avoids adding another dependency just to load
safe local runtime defaults.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

_LOADED_PATHS: set[Path] = set()


def _strip_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        return value[1:-1]
    return value


def load_repo_env(path: Optional[str | Path] = None, *, override: bool = False) -> None:
    env_path = Path(path) if path is not None else Path(__file__).resolve().parent / ".env"
    env_path = env_path.resolve()
    if env_path in _LOADED_PATHS:
        return
    _LOADED_PATHS.add(env_path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        key, sep, value = line.partition("=")
        key = key.strip()
        if not sep or not key or key.startswith("#"):
            continue
        if not override and key in os.environ:
            continue
        os.environ[key] = _strip_quotes(value)

