"""The database engine and how the rest of the code gets a session.

One engine per process. It owns a pool of open connections to Postgres,
and everything else borrows a connection from it rather than opening
its own.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import lru_cache

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from djq.config import get_settings


@lru_cache(maxsize=1) # means: run this function once, remember the answer, hand the same answer back forever after
def get_engine() -> AsyncEngine:
    """Built the first time it's asked for, then shared. Lazy on purpose:
    importing this module should never open a socket."""
    settings = get_settings()
    return create_async_engine(
        settings.database_url,
        pool_size=settings.pool_size,
        max_overflow=settings.pool_max_overflow,
        pool_timeout=settings.pool_timeout_seconds,
        # Cheap check that a pooled connection is still alive before handing
        # it out. Without this, a connection killed by a restart or a network
        # blip surfaces as a confusing error in the middle of real work.
        pool_pre_ping=True,
        # Retire connections after 30 minutes so none of them live forever.
        pool_recycle=1800,
        echo=False,
    )


@lru_cache(maxsize=1)
def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    """A factory for sessions. A session is one unit of work: it holds a
    connection, tracks the objects you've loaded, and wraps a transaction."""
    return async_sessionmaker(
        bind=get_engine(),
        class_=AsyncSession,
        # Keep loaded objects readable after commit. Without this, touching
        # `job.status` after committing fires another query, which in async
        # code raises error instead of quietly reloading.
        expire_on_commit=False,
        # Don't auto-send pending changes before every query. For a queue,
        # when writes hit the database is something we want to control.
        autoflush=False,
    )


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """One session wrapping one transaction.

    Commits if the block finishes cleanly, rolls back if anything raises,
    and closes either way. This is how non-web code (workers, scripts)
    should talk to the database.
    """
    async with get_sessionmaker()() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def dispose_engine() -> None:
    """Close every pooled connection and forget the engine.

    Call this on shutdown, and in tests between runs. asyncpg connections
    are tied to the event loop that created them, so reusing an engine
    across loops causes errors that look like nothing to do with the loop.
    """
    if get_engine.cache_info().currsize:
        await get_engine().dispose()
    get_engine.cache_clear()
    get_sessionmaker.cache_clear()