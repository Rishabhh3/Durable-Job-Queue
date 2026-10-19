"""Tests for the reaper.

Trick used throughout: we don't actually wait for a lease to expire. We claim
a job normally, then reach into the database and shove `lease_expires_at` into
the past. To the reaper that looks exactly like a worker that died.

Every helper opens its own session_scope, so each step commits before the next
one starts — the same way separate worker processes would behave in real life.
"""

from __future__ import annotations

from sqlalchemy import text

from djq.db import session_scope
from djq.queue import claim, enqueue
from djq.reaper import reap

# ---------------------------------------------------------------- helpers


async def _enqueue(max_attempts: int) -> int:
    async with session_scope() as session:
        return await enqueue(
            session,
            "greet",
            {"name": "rishabh"},
            max_attempts=max_attempts,
        )

async def _claim(worker_id: str) -> int | None:
    """Claim one job and return its id (or None if nothing was waiting)."""
    async with session_scope() as session:
        jobs = await claim(session, worker_id=worker_id)
        return jobs[0]["id"] if jobs else None


async def _reap() -> list[tuple[int, str]]:
    async with session_scope() as session:
        return await reap(session)


async def _expire_lease(job_id: int) -> None:
    """Pretend this job's lease ran out a minute ago."""
    async with session_scope() as session:
        await session.execute(
            text(
                "UPDATE jobs "
                "SET lease_expires_at = now() - interval '1 minute' "
                "WHERE id = :id"
            ),
            {"id": job_id},
        )


async def _read(job_id: int):
    async with session_scope() as session:
        result = await session.execute(
            text(
                "SELECT status, attempts, locked_by, lease_expires_at "
                "FROM jobs WHERE id = :id"
            ),
            {"id": job_id},
        )
        return result.one()


# ------------------------------------------------------------------ tests


async def test_reaper_requeues_job_with_attempts_left() -> None:
    job_id = await _enqueue(max_attempts=3)
    assert await _claim("worker-a") == job_id

    before = await _read(job_id)
    assert before.status == "running"
    assert before.attempts == 1

    await _expire_lease(job_id)

    assert (job_id, "pending") in await _reap()

    after = await _read(job_id)
    assert after.status == "pending"
    assert after.attempts == 1  # NOT bumped again by the reaper
    assert after.locked_by is None
    assert after.lease_expires_at is None


async def test_reaped_job_can_be_claimed_again() -> None:
    job_id = await _enqueue(max_attempts=3)
    await _claim("worker-a")
    await _expire_lease(job_id)
    await _reap()

    assert await _claim("worker-b") == job_id

    row = await _read(job_id)
    assert row.status == "running"
    assert row.attempts == 2
    assert row.locked_by == "worker-b"


async def test_reaper_kills_job_with_no_attempts_left() -> None:
    job_id = await _enqueue(max_attempts=1)
    await _claim("worker-a")
    await _expire_lease(job_id)

    assert (job_id, "dead") in await _reap()

    row = await _read(job_id)
    assert row.status == "dead"
    assert row.locked_by is None


async def test_reaper_leaves_healthy_jobs_alone() -> None:
    job_id = await _enqueue(max_attempts=3)
    await _claim("worker-a")

    # Lease is still valid, so this job is none of the reaper's business.
    assert all(rid != job_id for rid, _ in await _reap())

    row = await _read(job_id)
    assert row.status == "running"
    assert row.locked_by == "worker-a"


async def test_reaper_on_empty_table_does_nothing() -> None:
    assert await _reap() == []