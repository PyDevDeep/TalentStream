import uuid
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from app.config import get_settings

_engine = None
_session_factory = None


def get_engine():  # type: ignore[no-untyped-def]
    """Return the shared async engine, creating it on first call."""
    global _engine
    if _engine is None:
        database_url = str(get_settings().database_url)
        _engine = create_async_engine(
            database_url,
            poolclass=NullPool,
            echo=False,
            connect_args={
                "statement_cache_size": 0,
                "prepared_statement_cache_size": 0,
                "prepared_statement_name_func": lambda: f"__asyncpg_{uuid.uuid4()}__",
            },
        )
    return _engine


def _get_session_factory():  # type: ignore[no-untyped-def]
    """Return the shared session factory, creating it on first call."""
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            bind=get_engine(),
            expire_on_commit=False,
            class_=AsyncSession,
        )
    return _session_factory


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Yield an AsyncSession with automatic commit on success and rollback on error."""
    async with _get_session_factory()() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
