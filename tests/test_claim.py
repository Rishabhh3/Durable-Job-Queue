"""Proving two workers never get the same job."""

from __future__ import annotations

import asyncio

import pytest

from djq.db import get_sessionmaker
from djq.queue import claim, complete, enqueue, fail


async def test_claim_returns_a_job():
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        await enqueue(session, "send_email", {"to": "a@b.com"})
        await session.commit()

    async with sessionmaker() as session:
        claimed = await claim(session, worker_id="w1", limit=1)
        await session.commit()

    assert len(claimed) == 1
    assert claimed[0]["job_type"] == "send_email"
    assert claimed[0]["attempts"] == 1


async def test_claim_skips_future_jobs():
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        await enqueue(session, "later", delay_seconds=60)
        await session.commit()

    async with sessionmaker() as session:
        claimed = await claim(session, worker_id="w1", limit=10)
        await session.commit()

    assert claimed == []


async def test_enqueue_is_idempotent():
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        first = await enqueue(session, "once", idempotency_key="abc")
        second = await enqueue(session, "once", idempotency_key="abc")
        await session.commit()

    assert first == second


@pytest.mark.slow
async def test_eight_workers_never_claim_the_same_job():
    """The central claim of the project.

    A hundred jobs, eight workers grabbing as fast as they can. Every job
    must be claimed exactly once, by exactly one worker.
    """
    job_count = 100
    worker_count = 8
    sessionmaker = get_sessionmaker()

    async with sessionmaker() as session:
        for i in range(job_count):
            await enqueue(session, "work", {"n": i})
        await session.commit()

    async def worker(worker_id: str) -> list[int]:
        """Claim until the queue comes up empty."""
        mine: list[int] = []
        while True:
            async with sessionmaker() as session:
                claimed = await claim(session, worker_id=worker_id, limit=5)
                await session.commit()
            if not claimed:
                return mine
            mine.extend(job["id"] for job in claimed)
            # Let the other workers get a turn.
            await asyncio.sleep(0)

    results = await asyncio.gather(
        *(worker(f"w{i}") for i in range(worker_count))
    )

    all_claimed = [job_id for result in results for job_id in result]

    # Nothing claimed twice.
    assert len(all_claimed) == len(set(all_claimed)), "a job was claimed twice"
    # Nothing missed.
    assert len(all_claimed) == job_count
    # And more than one worker actually did something, or the test proved
    # nothing about concurrency.
    assert sum(1 for r in results if r) > 1

    async def test_complete_marks_job_succeeded():
        sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        job_id = await enqueue(session, "work")
        await session.commit()

    async with sessionmaker() as session:
        claimed = await claim(session, worker_id="w1", limit=1)
        ok = await complete(session, claimed[0]["id"], worker_id="w1")
        await session.commit()

    assert ok is True


async def test_complete_fails_if_worker_lost_the_job():
    """A worker that lost its lease must not be able to report success."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        job_id = await enqueue(session, "work")
        await session.commit()

    async with sessionmaker() as session:
        claimed = await claim(session, worker_id="w1", limit=1)
        # A different worker tries to complete it.
        ok = await complete(session, claimed[0]["id"], worker_id="w2")
        await session.commit()

    assert ok is False


async def test_failure_reschedules_then_goes_dead():
    """Fail a job until it runs out of attempts."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        await enqueue(session, "flaky", max_attempts=2)
        await session.commit()

    # First failure: back to pending.
    async with sessionmaker() as session:
        claimed = await claim(session, worker_id="w1", limit=1)
        result = await fail(session, claimed[0]["id"], "w1", "boom")
        await session.commit()
    assert result["status"] == "pending"

    # It's scheduled in the future, so it can't be claimed right now.
    async with sessionmaker() as session:
        assert await claim(session, worker_id="w1", limit=1) == []
        await session.commit()