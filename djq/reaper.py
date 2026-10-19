"""Reaper: recovers jobs whose owning worker died.

A worker stamps `locked_by` and `lease_expires_at` when it claims a job, and
keeps pushing `lease_expires_at` forward while it works. If the worker dies,
nothing pushes it forward and the row stays stuck in 'running' forever.

The reaper finds those rows and hands them back:
  - attempts still left  -> status 'pending', available immediately
  - attempts used up     -> status 'dead'

It never increments `attempts`. The attempt was already counted at claim time,
so counting again here would charge the job twice for one try.

Like the functions in queue.py, `reap` takes a session and leaves the
transaction boundary to the caller.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from djq.db import session_scope

log = logging.getLogger(__name__)

# One statement does the whole job. Postgres locks each row as it updates it,
# so two reapers running at once can never both reap the same row.
REAP_SQL = text(
    """
    UPDATE jobs
    SET
        status           = CASE
                               WHEN attempts >= max_attempts THEN 'dead'
                               ELSE 'pending'
                           END,
        run_at           = CASE
                               WHEN attempts >= max_attempts THEN run_at
                               ELSE now()
                           END,
        finished_at      = CASE
                               WHEN attempts >= max_attempts THEN now()
                               ELSE NULL
                           END,
        locked_by        = NULL,
        lease_expires_at = NULL,
        last_error       = 'lease expired: worker presumed dead',
        updated_at       = now()
    WHERE status = 'running'
      AND lease_expires_at < now()
    RETURNING id, status
    """
)


async def reap(session: AsyncSession) -> list[tuple[int, str]]:
    """Run one pass. Returns (job_id, new_status) for every job recovered."""
    result = await session.execute(REAP_SQL)
    return [(row.id, row.status) for row in result]


async def run_reaper(interval: float = 5.0) -> None:
    """Loop forever, reaping every `interval` seconds, until asked to stop."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    log.info("reaper started (every %.1fs)", interval)

    while not stop.is_set():
        try:
            async with session_scope() as session:
                recovered = await reap(session)
            for job_id, status in recovered:
                log.warning("reaped job %s -> %s", job_id, status)
        except Exception:
            # One bad pass must not kill the loop. Log it and try again.
            log.exception("reaper pass failed")

        # Sleep, but wake up straight away if we've been asked to shut down.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval)

    log.info("reaper stopped")


def main() -> None:
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run_reaper())


if __name__ == "__main__":
    main()