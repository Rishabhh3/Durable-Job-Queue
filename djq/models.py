"""Database tables.
It tells what the table should look like. Alembic turns it into real SQL and sends it to Postgres, creating
table that has these columns.
So it is - create the jobs table with these cols, constraints, indexes"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger, CheckConstraint, DateTime, Index, Integer, String, Text, func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "jobs"
    # left is for python , right is for Postgres
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # --- what to run ---
    job_type: Mapped[str] = mapped_column(String(100), nullable=False)
    args: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}") # giving empty object rather than null
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    # str | None = optional

    # --- where it is in its life ---
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default="pending"
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # --- timing and ownership ---
    run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    locked_by: Mapped[str | None] = mapped_column(String(100), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # --- record keeping ---
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'running', 'succeeded', 'dead')",
            name="ck_jobs_status",
            # This is Postgres refusing to store a status that isn't one of those four. A typo like "suceeded" fails at insert time rather than silently creating a job that no query will ever find again.

        ),
        CheckConstraint("attempts >= 0", name="ck_jobs_attempts_non_negative"),
        CheckConstraint("max_attempts >= 1", name="ck_jobs_max_attempts_positive"),

        # Claiming: find the oldest job that is ready to run.
        Index(
            "ix_jobs_claim",
            "run_at",
            postgresql_where=(status == "pending"),
        ),
        # Reclaiming: find jobs whose holder has gone quiet.
        Index(
            "ix_jobs_expired_leases",
            "lease_expires_at",
            postgresql_where=(status == "running"),
        ),
        # Same key twice means the same job, only once.
        Index(
            "ix_jobs_idempotency_key",
            "idempotency_key",
            unique=True,
            postgresql_where=(idempotency_key.isnot(None)),
        ),
    )