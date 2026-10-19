"""HTTP control surface for the queue.

Submit jobs, look at them, see what died, put a dead job back, and check
whether the queue is keeping up.

Deliberately not here: running jobs. The API only writes rows. Workers are
separate processes, so restarting the web server never kills work in flight.

No SQL in this file either — every database query lives in queue.py, so
there's one place that knows the schema.

Run it with:  uvicorn djq.api:app --reload
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from djq.db import dispose_engine, session_scope
from djq.queue import enqueue, get_job, list_dead, requeue, stats

log = logging.getLogger("djq.api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    yield
    await dispose_engine()


app = FastAPI(
    title="Durable Job Queue",
    description="A job queue built on plain Postgres.",
    version="0.1.0",
    lifespan=lifespan,
)


async def get_db() -> AsyncIterator[AsyncSession]:
    """One session per request, committed or rolled back when it ends."""
    async with session_scope() as session:
        yield session


# ------------------------------------------------------------------ shapes


class SubmitRequest(BaseModel):
    job_type: str = Field(..., min_length=1, max_length=100)
    args: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(
        default=None,
        max_length=255,
        description=(
            "Send the same key twice and the job is only created once. "
            "Stops duplicate submissions; does not stop duplicate runs."
        ),
    )
    max_attempts: int | None = Field(default=None, ge=1, le=100)
    delay_seconds: float = Field(default=0.0, ge=0)


class JobOut(BaseModel):
    # extra="ignore" so adding a column to the table doesn't break this.
    model_config = ConfigDict(extra="ignore")

    id: int
    job_type: str
    args: Any = None
    status: str
    attempts: int
    max_attempts: int
    run_at: datetime | None = None
    lease_expires_at: datetime | None = None
    finished_at: datetime | None = None
    locked_by: str | None = None
    last_error: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class StatsOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    pending: int
    ready_now: int
    running: int
    succeeded: int
    dead: int
    awaiting_reaper: int
    oldest_wait_seconds: float
    retried: int
    retry_rate: float


# ------------------------------------------------------------------ routes


# There's nothing to show at the root, but a browser pointed at the base URL
# shouldn't get a bare 404. Send it to the generated API docs instead.
@app.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    return RedirectResponse("/docs")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/jobs", response_model=JobOut, status_code=201)
async def submit_job(
    body: SubmitRequest,
    session: AsyncSession = Depends(get_db),
) -> dict:
    """Put a job in the queue.

    With an idempotency_key, a repeat submission returns the job that got
    there first instead of creating a second one.
    """
    job_id = await enqueue(
        session,
        job_type=body.job_type,
        args=body.args,
        idempotency_key=body.idempotency_key,
        max_attempts=body.max_attempts,
        delay_seconds=body.delay_seconds,
    )
    job = await get_job(session, job_id)
    if job is None:  # pragma: no cover - should be impossible
        raise HTTPException(500, "job vanished immediately after insert")
    return job


# Declared before /jobs/{job_id} so the word "dead" is never read as an id.
@app.get("/jobs/dead", response_model=list[JobOut])
async def dead_jobs(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_db),
) -> list[dict]:
    """Jobs that used up every attempt. The dead-letter queue."""
    return await list_dead(session, limit=limit, offset=offset)


@app.get("/jobs/{job_id}", response_model=JobOut)
async def job_detail(
    job_id: int,
    session: AsyncSession = Depends(get_db),
) -> dict:
    job = await get_job(session, job_id)
    if job is None:
        raise HTTPException(404, f"no job with id {job_id}")
    return job


@app.post("/jobs/{job_id}/retry", response_model=JobOut)
async def retry_job(
    job_id: int,
    session: AsyncSession = Depends(get_db),
) -> dict:
    """Send a dead job back to the queue with a fresh set of attempts.

    Only works on dead jobs. A job still pending or running is already on
    its way and doesn't need pushing.
    """
    if not await requeue(session, job_id):
        job = await get_job(session, job_id)
        if job is None:
            raise HTTPException(404, f"no job with id {job_id}")
        raise HTTPException(
            409, f"job {job_id} is {job['status']}, not dead"
        )
    job = await get_job(session, job_id)
    assert job is not None
    return job


@app.get("/stats", response_model=StatsOut)
async def queue_stats(
    session: AsyncSession = Depends(get_db),
) -> dict:
    """How the queue is doing.

    oldest_wait_seconds is the number to watch: it answers "are workers
    keeping up" better than queue depth does, because depth alone can't
    tell a busy queue from a stuck one.
    """
    return await stats(session)