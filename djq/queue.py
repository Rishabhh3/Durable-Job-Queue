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

COMPLETE_SQL = text("""
    UPDATE jobs
    SET status           = 'succeeded',
        finished_at      = now(),
        locked_by        = NULL,
        lease_expires_at = NULL,
        last_error       = NULL,
        updated_at       = now()
    WHERE id = :job_id
      AND status = 'running'
      AND locked_by = :worker_id
    RETURNING id
""")


FAIL_SQL = text("""
    UPDATE jobs
    SET status = CASE
            WHEN attempts >= max_attempts THEN 'dead'
            ELSE 'pending'
        END,
        run_at = CASE
            WHEN attempts >= max_attempts THEN run_at
            ELSE now() + make_interval(secs => least(
                power(:backoff_base, attempts),
                :backoff_max
            ))
        END,
        finished_at = CASE
            WHEN attempts >= max_attempts THEN now()
            ELSE NULL
        END,
        last_error       = :error,
        locked_by        = NULL,
        lease_expires_at = NULL,
        updated_at       = now()
    WHERE id = :job_id
      AND status = 'running'
      AND locked_by = :worker_id
    RETURNING id, status, run_at
""")


async def complete(session: AsyncSession, job_id: int, worker_id: str) -> bool:
    """Mark a job done. Returns False if this worker no longer holds it."""
    result = await session.execute(
        COMPLETE_SQL, {"job_id": job_id, "worker_id": worker_id}
    )
    return result.first() is not None


async def fail(
    session: AsyncSession,
    job_id: int,
    worker_id: str,
    error: str,
) -> dict | None:
    """Record a failure.

    Puts the job back in the queue with a growing delay, or marks it dead
    if it has used up its attempts. Returns None if this worker no longer
    holds the job.
    """
    settings = get_settings()
    result = await session.execute(
        FAIL_SQL,
        {
            "job_id": job_id,
            "worker_id": worker_id,
            "error": error[:5000],
            "backoff_base": settings.backoff_base_seconds,
            "backoff_max": settings.backoff_max_seconds,
        },
    )
    row = result.mappings().first()
    return dict(row) if row is not None else None

RENEW_SQL = text("""
    UPDATE jobs
    SET lease_expires_at = now() + make_interval(secs => :lease_seconds),
        updated_at       = now()
    WHERE id = :job_id
      AND status = 'running'
      AND locked_by = :worker_id
    RETURNING id
""")


async def renew_lease(
    session: AsyncSession,
    job_id: int,
    worker_id: str,
    lease_seconds: float | None = None,
) -> bool:
    """Push the lease further out. False means this worker lost the job."""
    settings = get_settings()
    result = await session.execute(
        RENEW_SQL,
        {
            "job_id": job_id,
            "worker_id": worker_id,
            "lease_seconds": (
                lease_seconds if lease_seconds is not None
                else settings.lease_seconds
            ),
        },
    )
    return result.first() is not None
# --------------------------------------------------------------- read side
#
# Everything below is for the HTTP layer in api.py. None of it is on the
# worker's hot path, so these favour being easy to read over being fast.

GET_JOB_SQL = text("""
    SELECT *
    FROM jobs
    WHERE id = :job_id
""")


async def get_job(session: AsyncSession, job_id: int) -> dict | None:
    """One job as a plain dict, or None if there's no such id."""
    result = await session.execute(GET_JOB_SQL, {"job_id": job_id})
    row = result.mappings().first()
    return dict(row) if row is not None else None


LIST_DEAD_SQL = text("""
    SELECT *
    FROM jobs
    WHERE status = 'dead'
    ORDER BY finished_at DESC NULLS LAST, id DESC
    LIMIT :limit
    OFFSET :offset
""")


async def list_dead(
    session: AsyncSession,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """The dead-letter queue, most recently died first."""
    result = await session.execute(
        LIST_DEAD_SQL, {"limit": limit, "offset": offset}
    )
    return [dict(row) for row in result.mappings()]


# Resets attempts to 0, so a requeued job gets its full budget again rather
# than dying on the first failure. The status guard means this is safe to
# call twice: the second call matches no row and returns False.
REQUEUE_SQL = text("""
    UPDATE jobs
    SET status           = 'pending',
        attempts         = 0,
        run_at           = now(),
        locked_by        = NULL,
        lease_expires_at = NULL,
        last_error       = NULL,
        finished_at      = NULL,
        updated_at       = now()
    WHERE id = :job_id
      AND status = 'dead'
    RETURNING id
""")


async def requeue(session: AsyncSession, job_id: int) -> bool:
    """Send a dead job back to the queue. False if it wasn't dead."""
    result = await session.execute(REQUEUE_SQL, {"job_id": job_id})
    return result.first() is not None


# One pass over the table. count(*) FILTER is Postgres' way of counting
# several different things without scanning several times.
STATS_SQL = text("""
    SELECT
        count(*) FILTER (WHERE status = 'pending')              AS pending,
        count(*) FILTER (WHERE status = 'pending'
                           AND run_at <= now())                 AS ready_now,
        count(*) FILTER (WHERE status = 'running')              AS running,
        count(*) FILTER (WHERE status = 'succeeded')            AS succeeded,
        count(*) FILTER (WHERE status = 'dead')                 AS dead,
        count(*) FILTER (WHERE status = 'running'
                           AND lease_expires_at < now())        AS awaiting_reaper,
        COALESCE(
            EXTRACT(EPOCH FROM (
                now() - min(run_at) FILTER (WHERE status = 'pending'
                                              AND run_at <= now())
            )),
            0
        )                                                       AS oldest_wait_seconds,
        count(*) FILTER (WHERE attempts > 1)                    AS retried,
        COALESCE(
            count(*) FILTER (WHERE attempts > 1)::float
                / NULLIF(count(*) FILTER (WHERE attempts > 0), 0),
            0
        )                                                       AS retry_rate
    FROM jobs
""")


async def stats(session: AsyncSession) -> dict:
    """A snapshot of the queue for monitoring.

    `oldest_wait_seconds` is the one to alert on: it's how long the job at
    the front of the queue has been waiting past its run_at. Queue depth
    alone can't tell a busy queue from a stuck one, this can.

    `retry_rate` is the share of started jobs that needed more than one
    attempt, so a job still sitting in the queue doesn't drag it down.
    """
    result = await session.execute(STATS_SQL)
    row = result.mappings().one()
    return dict(row)
