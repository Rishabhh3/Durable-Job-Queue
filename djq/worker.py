"""The worker process: claim a job, run it, report what happened."""

from __future__ import annotations

import asyncio
import logging
import os
import random
import signal
import socket
import traceback
from collections.abc import Awaitable, Callable
from typing import Any

from djq.config import get_settings
from djq.db import dispose_engine, session_scope
from djq.queue import claim, complete, fail, renew_lease

log = logging.getLogger("djq.worker")

Handler = Callable[[dict[str, Any]], Awaitable[None]]

# Which function runs which job_type.
HANDLERS: dict[str, Handler] = {}


def register(job_type: str) -> Callable[[Handler], Handler]:
    """Decorator. Attaches a function to a job_type."""
    def wrap(fn: Handler) -> Handler:
        HANDLERS[job_type] = fn
        return fn
    return wrap


def make_worker_id() -> str:
    """Something that identifies this process, for locked_by and for logs."""
    return f"{socket.gethostname()}-{os.getpid()}"


async def _heartbeat(job_id: int, worker_id: str, stop: asyncio.Event) -> None:
    """Renew the lease on a loop until told to stop.

    Runs alongside the job. If renewal ever fails, this worker has lost the
    job and there is no point holding on to it.
    """
    settings = get_settings()
    while not stop.is_set():
        try:
            await asyncio.wait_for(
                stop.wait(), timeout=settings.heartbeat_seconds
            )
            return  # stop was set; job finished normally
        except TimeoutError:
            pass  # time to renew

        async with session_scope() as session:
            still_ours = await renew_lease(session, job_id, worker_id)

        if not still_ours:
            log.warning("lost lease on job %s; another worker has it", job_id)
            return


async def _run_one(job: dict[str, Any], worker_id: str) -> None:
    """Run a single job and record the outcome."""
    job_id = job["id"]
    handler = HANDLERS.get(job["job_type"])

    if handler is None:
        async with session_scope() as session:
            await fail(
                session, job_id, worker_id,
                f"no handler registered for {job['job_type']!r}",
            )
        return

    stop = asyncio.Event()
    beat = asyncio.create_task(_heartbeat(job_id, worker_id, stop))

    try:
        await handler(job["args"])
    except Exception:
        # The traceback is far more useful than the message alone.
        async with session_scope() as session:
            outcome = await fail(
                session, job_id, worker_id, traceback.format_exc()
            )
        if outcome:
            log.warning("job %s failed, now %s", job_id, outcome["status"])
    else:
        async with session_scope() as session:
            ok = await complete(session, job_id, worker_id)
        if ok:
            log.info("job %s done", job_id)
        else:
            # Finished the work but lost the lease partway. Someone else
            # may well run it again, which is why handlers should be safe
            # to run twice.
            log.warning("job %s finished but lease was lost", job_id)
    finally:
        stop.set()
        await beat


async def run(worker_id: str | None = None) -> None:
    """The main loop. Claim, run, repeat. Stops on Ctrl-C or SIGTERM."""
    settings = get_settings()
    worker_id = worker_id or make_worker_id()
    shutdown = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    log.info("worker %s started", worker_id)

    try:
        while not shutdown.is_set():
            async with session_scope() as session:
                jobs = await claim(
                    session, worker_id, limit=settings.batch_size
                )

            if not jobs:
                # Nothing to do. Sleep with a bit of randomness so many
                # workers don't all wake at the same instant.
                wobble = 1 + random.uniform(0, settings.poll_jitter)
                delay = settings.poll_interval_seconds * wobble
                try:
                    await asyncio.wait_for(shutdown.wait(), timeout=delay)
                except TimeoutError:
                    pass
                continue

            for job in jobs:
                await _run_one(job, worker_id)
                if shutdown.is_set():
                    break
    finally:
        log.info("worker %s stopping", worker_id)
        await dispose_engine()


def main() -> None:
    """Entry point. load_dotenv lives here, not in config."""
    from dotenv import load_dotenv

    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()