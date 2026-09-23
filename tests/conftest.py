"""Test setup: a real Postgres database, migrated, cleaned between tests."""

from __future__ import annotations

import os

import pytest

# Point everything at the test database before any settings are read.
os.environ["DJQ_DATABASE_URL"] = (
    "postgresql+asyncpg://djq:djq@127.0.0.1:5432/djq_test"
)
# The concurrency tests run more workers than the default pool allows.
os.environ["DJQ_POOL_SIZE"] = "20"

from alembic import command  # noqa: E402
from alembic.config import Config  # noqa: E402
from sqlalchemy import text  # noqa: E402

from djq.config import get_settings  # noqa: E402
from djq.db import dispose_engine, get_sessionmaker  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def migrate() -> None:
    """Bring the test database up to date once per run.

    Migrations, not create_all. Running the real migrations means the
    deployment path is tested too, instead of only the models.
    """
    get_settings.cache_clear()
    command.upgrade(Config("alembic.ini"), "head")


@pytest.fixture(autouse=True)
async def clean_table():
    """Empty the table before each test so tests can't affect each other."""
    async with get_sessionmaker()() as session:
        await session.execute(text("TRUNCATE jobs RESTART IDENTITY"))
        await session.commit()
    yield


@pytest.fixture(scope="session", autouse=True)
async def close_engine():
    """Close pooled connections at the end of the run."""
    yield
    await dispose_engine()