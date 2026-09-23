"""Putting work in and taking work out.

The claim query is the heart of this project. Everything else is
bookkeeping around it.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from djq.config import get_settings

import json

CLAIM_SQL = text("""
    WITH ready AS (
        SELECT id
        FROM jobs
        WHERE status = 'pending'
          AND run_at <= now()
        ORDER BY run_at
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
    )
    UPDATE jobs
    SET status           = 'running',
        locked_by        = :worker_id,
        lease_expires_at = now() + make_interval(secs => :lease_seconds),
        attempts         = attempts + 1,
        updated_at       = now()
    WHERE id IN (SELECT id FROM ready)
    RETURNING id, job_type, args, attempts, max_attempts, lease_expires_at
""")


async def claim(
    session: AsyncSession,
    worker_id: str,
    limit: int | None = None,
    lease_seconds: float | None = None,
) -> list[dict]:
    """Take up to `limit` jobs for this worker.

    Returns the claimed jobs, which may be fewer than asked for, or none
    at all if the queue is empty or every ready job is already held.
    """
    settings = get_settings()
    result = await session.execute(
        CLAIM_SQL,
        {
            "limit": limit if limit is not None else settings.batch_size,
            "worker_id": worker_id,
            "lease_seconds": (
                lease_seconds if lease_seconds is not None
                else settings.lease_seconds
            ),
        },
    )
    return [dict(row) for row in result.mappings()]

INSERT_SQL = text("""
    INSERT INTO jobs (job_type, args, idempotency_key, max_attempts, run_at)
    VALUES (
        :job_type,
        CAST(:args AS jsonb),
        :idempotency_key,
        :max_attempts,
        now() + make_interval(secs => :delay_seconds)
    )
    ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
    DO NOTHING
    RETURNING id
""")

FIND_BY_KEY_SQL = text("""
    SELECT id FROM jobs WHERE idempotency_key = :idempotency_key
""")


async def enqueue(
    session: AsyncSession,
    job_type: str,
    args: dict | None = None,
    idempotency_key: str | None = None,
    max_attempts: int | None = None,
    delay_seconds: float = 0.0,
) -> int:
    """Put a job in the queue and return its id.

    If `idempotency_key` matches a job already in the table, nothing is
    inserted and the existing job's id comes back instead.
    """
    settings = get_settings()
    params = {
        "job_type": job_type,
        "args": json.dumps(args or {}),
        "idempotency_key": idempotency_key,
        "max_attempts": (
            max_attempts if max_attempts is not None else settings.max_attempts
        ),
        "delay_seconds": delay_seconds,
    }

    result = await session.execute(INSERT_SQL, params)
    row = result.first()
    if row is not None:
        return row.id

    # DO NOTHING returns no row, so the key was already taken. Look up
    # the job that got there first.
    existing = await session.execute(
        FIND_BY_KEY_SQL, {"idempotency_key": idempotency_key}
    )
    return existing.scalar_one()