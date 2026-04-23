from datetime import datetime
from typing import List, Optional

from sqlalchemy import Index, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    external_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    title: Mapped[str] = mapped_column(String)
    company: Mapped[str] = mapped_column(String)
    location: Mapped[Optional[str]] = mapped_column(String)
    salary_min: Mapped[Optional[int]]
    salary_max: Mapped[Optional[int]]
    salary_currency: Mapped[str] = mapped_column(String, server_default="USD")

    skills: Mapped[List[str]] = mapped_column(JSONB, server_default="[]")

    description_snippet: Mapped[Optional[str]] = mapped_column(String(500))
    source_url: Mapped[str] = mapped_column(Text)
    notified: Mapped[bool] = mapped_column(server_default=text("false"))

    created_at: Mapped[datetime] = mapped_column(server_default=func.now(), index=True)
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())

    # Partial index to speed up queries for unnotified jobs.
    __table_args__ = (
        Index(
            "ix_jobs_notified_false",
            "notified",
            postgresql_where=text("notified = false"),
        ),
    )
