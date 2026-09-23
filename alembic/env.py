"""How Alembic connects to the database and finds your tables.

Two things this file has to supply: where the database is (from your
settings, never from alembic.ini), and what the tables are supposed to
look like (from your models).
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from dotenv import load_dotenv
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

# This is an entry point, so .env gets loaded here, before anything reads
# the environment. Same rule as main.py and worker.py.
load_dotenv()

from djq.config import get_settings  # noqa: E402  (must come after load_dotenv)
from djq.models import Base  # noqa: E402

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# The picture of what the tables *should* be. Autogenerate compares this
# against the real database. Importing Base isn't enough on its own —
# importing djq.models is what registers the Job table onto it.
target_metadata = Base.metadata


def _url() -> str:
    return get_settings().database_url


def _configure(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        # Notice when a column's type changes, not just when columns
        # appear or vanish.
        compare_type=True,
        # Notice when a server_default changes. You use several, so
        # without this they'd drift silently.
        compare_server_default=True,
    )


def run_migrations_offline() -> None:
    """Emit SQL to stdout instead of running it.

    Used with `alembic upgrade head --sql` when someone else applies the
    change, or when you want to read the SQL before trusting it.
    """
    context.configure(
        url=_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Connect for real and apply the migrations."""
    # A throwaway engine with no pooling. Migrations run once and exit,
    # so keeping connections around would be pointless, and reusing the
    # app's pool here just tangles two lifecycles together.
    engine = create_async_engine(_url(), poolclass=pool.NullPool)

    async with engine.connect() as connection:
        await connection.run_sync(_do_run_migrations)

    await engine.dispose()


def _do_run_migrations(connection: Connection) -> None:
    """Alembic's internals are synchronous, so run_sync hands us a plain
    connection to work with inside the async one."""
    _configure(connection)
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())