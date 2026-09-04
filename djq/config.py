"""Application settings, loaded once from the environment at startup."""

from __future__ import annotations

import os
from dataclasses import dataclass

DEV_DATABASE_URL = "postgresql+asyncpg://djq:djq@localhost:5432/djq_dev"
''' address that python code use to reach postgres, :// username/password@localhost:port/db_name'''

@dataclass(frozen=True) # frozen- keeps the objects read only after creation, something might raise an error instead of silently working
# if any modify the value it is bug, so what I load at startup is what everyone sees
class Settings:
    # --- connection ---
    database_url: str = DEV_DATABASE_URL
    pool_size: int = 5
    pool_max_overflow: int = 0

    # --- queue semantics ---
    lease_seconds: float = 30.0
    heartbeat_seconds: float = 10.0
    max_attempts: int = 5
    backoff_base_seconds: float = 2.0
    backoff_max_seconds: float = 600.0

    # --- worker loop ---
    poll_interval_seconds: float = 1.0
    poll_jitter: float = 0.3
    batch_size: int = 1

    # --- environment ---
    env: str = "dev"

    def __post_init__(self) -> None:    # this is where validation is happening
        if self.heartbeat_seconds >= self.lease_seconds / 2:
            raise ValueError(
                f"heartbeat_seconds ({self.heartbeat_seconds}) must be less than "
                f"half of lease_seconds ({self.lease_seconds}); otherwise a single "
                f"missed heartbeat lets a healthy worker lose its job"
            )
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if not 0.0 <= self.poll_jitter < 1.0:
            raise ValueError("poll_jitter must be in [0.0, 1.0)")

# all these env read one enviroment variable
def _env_str(key: str, default: str) -> str:
    return os.environ.get(key, default)

''' these all basically mean give me this key or fallback if its missing 
if nothing is set all fall back to default
and everything in python is string so required to convert to int or float as per requirement'''

def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer, got {raw!r}") from exc


def _env_float(key: str, default: float) -> float:
    raw = os.environ.get(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{key} must be a number, got {raw!r}") from exc


def load_settings() -> Settings:
    """Build Settings from environment variables, falling back to defaults."""
    return Settings(
        database_url=_env_str("DJQ_DATABASE_URL", DEV_DATABASE_URL),
        pool_size=_env_int("DJQ_POOL_SIZE", 5),
        pool_max_overflow=_env_int("DJQ_POOL_MAX_OVERFLOW", 0),
        lease_seconds=_env_float("DJQ_LEASE_SECONDS", 30.0),
        heartbeat_seconds=_env_float("DJQ_HEARTBEAT_SECONDS", 10.0),
        max_attempts=_env_int("DJQ_MAX_ATTEMPTS", 5),
        backoff_base_seconds=_env_float("DJQ_BACKOFF_BASE_SECONDS", 2.0),
        backoff_max_seconds=_env_float("DJQ_BACKOFF_MAX_SECONDS", 600.0),
        poll_interval_seconds=_env_float("DJQ_POLL_INTERVAL_SECONDS", 1.0),
        poll_jitter=_env_float("DJQ_POLL_JITTER", 0.3),
        batch_size=_env_int("DJQ_BATCH_SIZE", 1),
        env=_env_str("DJQ_ENV", "dev"),
    )