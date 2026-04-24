"""
FastAPI Dependency Injection providers for DB session and broker access.

NOTE: These providers are designed for FastAPI route handlers via Depends().
TaskIQ workers use TaskiqState for shared resources (see app/broker.py).
"""

from typing import AsyncGenerator

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker import broker as _broker
from app.config import get_settings
from app.db.session import get_session as _get_session


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Yield an AsyncSession with automatic commit/rollback."""
    async with _get_session() as session:
        yield session


def get_broker():  # type: ignore[no-untyped-def]
    """Return the configured TaskIQ broker instance."""
    return _broker


async def get_redis() -> AsyncGenerator[Redis, None]:
    """Yield an async Redis client, closing it after use."""
    settings = get_settings()
    client: Redis = Redis.from_url(str(settings.redis_url))  # type: ignore[reportUnknownMemberType]
    try:
        yield client
    finally:
        await client.aclose()
