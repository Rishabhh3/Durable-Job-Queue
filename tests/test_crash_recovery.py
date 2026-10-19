"""The crash test: kill a real worker mid-job, prove the job still finishes.

Every other test fakes a dead worker by editing lease_expires_at with SQL.
This one starts an actual separate process, SIGKILLs it while it holds a job,
and shows the reaper plus a second worker recover the work with no help from
the dead process.

SIGKILL matters. SIGTERM would let the worker shut down tidily, which is the
case the code already handles. SIGKILL gives it no chance to run anything at
all, which is what a power cut or an OOM kill looks like.

The worker runs with a short lease (see LEASE_SECONDS) so the test waits a few
seconds rather than a full production lease. The lease still lapses on its
own, so nothing about the mechanism is faked.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from sqlalchemy import text

from djq.config import get_settings
from djq.db import session_scope
from djq.queue import enqueue
from djq.reaper import reap

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKER_SCRIPT = Path(__file__).resolve().parent / "crash_worker.py"

LEASE_SECONDS = 5.0
HEARTBEAT_SECONDS = 2.0

pytestmark = pytest.mark.slow


# ---------------------------------------------------------------- process


def _start_worker(sleep_seconds: float) -> subprocess.Popen:
    """Launch a real worker process pointed at the test database."""
    env = os.environ.copy()
    env["DJQ_DATABASE_URL"] = get_settings().database_url
    env["DJQ_LEASE_SECONDS"] = str(LEASE_SECONDS)
    env["DJQ_HEARTBEAT_SECONDS"] = str(HEARTBEAT_SECONDS)
    env["DJQ_CRASH_TEST_SLEEP"] = str(sleep_seconds)
    return subprocess.Popen(
        [sys.executable, "-u", str(WORKER_SCRIPT)],
        cwd=str(REPO_ROOT),
        env=env,
    )


def _kill_hard(proc: subprocess.Popen) -> None:
    """SIGKILL and wait for the process to actually be gone."""
    if proc.poll() is None:
        proc.kill()
    proc.wait(timeout=10)


def _stop_gently(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        _kill_hard(proc)


# --------------------------------------------------------------- database


async def _row(job_id: int):
    async with session_scope() as session:
        result = await session.execute(
            text(
                "SELECT status, attempts, locked_by, "
                "       lease_expires_at < now() AS lease_lapsed "
                "FROM jobs WHERE id = :id"
            ),
            {"id": job_id},
        )
        return result.one()


async def _wait_for(job_id: int, check, within: float, what: str):
    """Poll the row until check(row) is true. Fail the test if it never is."""
    deadline = time.monotonic() + within
    last = None
    while time.monotonic() < deadline:
        last = await _row(job_id)
        if check(last):
            return last
        await asyncio.sleep(0.2)
    pytest.fail(f"timed out waiting for {what}; last row was {last}")


async def _reap() -> list[tuple[int, str]]:
    async with session_scope() as session:
        return await reap(session)


# ------------------------------------------------------------------ test


async def test_job_survives_a_killed_worker() -> None:
    async with session_scope() as session:
        job_id = await enqueue(session, "crash_test", {}, max_attempts=3)

    # 1. A worker picks the job up and settles in for a long sleep.
    victim = _start_worker(sleep_seconds=60)
    try:
        running = await _wait_for(
            job_id,
            lambda r: r.status == "running",
            within=30,
            what="the first worker to claim the job",
        )
        assert running.locked_by is not None
        assert running.attempts == 1

        # 2. Pull the plug. No shutdown handler, no cleanup, nothing.
        _kill_hard(victim)
    finally:
        _kill_hard(victim)

    # 3. The job is stranded: marked running, owned by a process that no
    #    longer exists. Without a reaper it would sit here forever.
    stranded = await _row(job_id)
    assert stranded.status == "running"
    assert stranded.locked_by is not None

    # 4. Nobody is renewing the lease now, so it lapses on its own.
    await _wait_for(
        job_id,
        lambda r: r.lease_lapsed,
        within=LEASE_SECONDS + 15,
        what="the dead worker's lease to lapse",
    )

    # 5. The reaper notices and puts the job back in the queue.
    assert (job_id, "pending") in await _reap()

    requeued = await _row(job_id)
    assert requeued.status == "pending"
    assert requeued.attempts == 1  # the dead attempt is not charged twice
    assert requeued.locked_by is None

    # 6. A fresh worker picks it up and finishes it.
    rescuer = _start_worker(sleep_seconds=0)
    try:
        done = await _wait_for(
            job_id,
            lambda r: r.status == "succeeded",
            within=30,
            what="the second worker to finish the job",
        )
        assert done.attempts == 2  # proof it was genuinely claimed again
    finally:
        _stop_gently(rescuer)