"""Application settings, loaded once from the environment at startup."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache

# How Python reaches Postgres: driver://user:password@host:port/database
DEV_DATABASE_URL = "postgresql+asyncpg://djq:djq@127.0.0.1:5432/djq_dev"


@dataclass(frozen=True)
class Settings:
    """Read-only settings. Frozen so a stray assignment raises instead of
    silently changing behaviour for everyone holding this object."""

    # --- connection ---
    database_url: str = DEV_DATABASE_URL
    pool_size: int = 5
    pool_max_overflow: int = 0
    pool_timeout_seconds: float = 10.0

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

    def __post_init__(self) -> None:
        if self.lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than 0")
        if self.heartbeat_seconds <= 0:
            raise ValueError("heartbeat_seconds must be greater than 0")
        if self.heartbeat_seconds >= self.lease_seconds / 2:
            raise ValueError(
                f"heartbeat_seconds ({self.heartbeat_seconds}) must be less than "
                f"half of lease_seconds ({self.lease_seconds}); otherwise a single "
                f"missed heartbeat lets a healthy worker lose its job"
            )
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.backoff_base_seconds <= 1.0:
            raise ValueError(
                f"backoff_base_seconds ({self.backoff_base_seconds}) must be greater "
                f"than 1.0, otherwise retry delays never grow and a failing job "
                f"hammers the database"
            )
        if self.backoff_max_seconds < self.backoff_base_seconds:
            raise ValueError(
                f"backoff_max_seconds ({self.backoff_max_seconds}) cannot be smaller "
                f"than backoff_base_seconds ({self.backoff_base_seconds})"
            )
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be greater than 0")
        if not 0.0 <= self.poll_jitter < 1.0:
            raise ValueError("poll_jitter must be in [0.0, 1.0)")
        if self.batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if self.pool_size < 1:
            raise ValueError("pool_size must be at least 1")
        if self.pool_max_overflow < 0:
            raise ValueError("pool_max_overflow cannot be negative")
        if self.pool_timeout_seconds <= 0:
            raise ValueError("pool_timeout_seconds must be greater than 0")


def _raw(key: str) -> str | None:
    """Read one variable, treating blank as unset so a stray `KEY=` in a
    shell or compose file falls back to the default instead of exploding."""
    value = os.environ.get(key)
    if value is None or value.strip() == "":
        return None
    return value.strip()


def _env_str(key: str, default: str) -> str:
    raw = _raw(key)
    return default if raw is None else raw


def _env_int(key: str, default: int) -> int:
    raw = _raw(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer, got {raw!r}") from exc


def _env_float(key: str, default: float) -> float:
    raw = _raw(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{key} must be a number, got {raw!r}") from exc


def _resolve_database_url(env: str) -> str:
    """Outside dev, refuse to guess. A missing variable should crash loudly
    rather than quietly connecting to a throwaway dev database."""
    raw = _raw("DJQ_DATABASE_URL")
    if raw is not None:
        return raw
    if env != "dev":
        raise ValueError(
            f"DJQ_DATABASE_URL is required when DJQ_ENV is {env!r}; "
            f"the built-in default points at a local dev database"
        )
    return DEV_DATABASE_URL


def load_settings() -> Settings:
    """Build Settings from environment variables, falling back to defaults."""
    env = _env_str("DJQ_ENV", "dev")
    return Settings(
        database_url=_resolve_database_url(env),
        pool_size=_env_int("DJQ_POOL_SIZE", 5),
        pool_max_overflow=_env_int("DJQ_POOL_MAX_OVERFLOW", 0),
        pool_timeout_seconds=_env_float("DJQ_POOL_TIMEOUT_SECONDS", 10.0),
        lease_seconds=_env_float("DJQ_LEASE_SECONDS", 30.0),
        heartbeat_seconds=_env_float("DJQ_HEARTBEAT_SECONDS", 10.0),
        max_attempts=_env_int("DJQ_MAX_ATTEMPTS", 5),
        backoff_base_seconds=_env_float("DJQ_BACKOFF_BASE_SECONDS", 2.0),
        backoff_max_seconds=_env_float("DJQ_BACKOFF_MAX_SECONDS", 600.0),
        poll_interval_seconds=_env_float("DJQ_POLL_INTERVAL_SECONDS", 1.0),
        poll_jitter=_env_float("DJQ_POLL_JITTER", 0.3),
        batch_size=_env_int("DJQ_BATCH_SIZE", 1),
        env=env,
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached accessor. Call this everywhere so the whole process shares one
    object. Tests can reset it with `get_settings.cache_clear()`."""
    return load_settings()