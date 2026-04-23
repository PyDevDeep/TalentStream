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

settings = get_settings()

# Перетворюємо PostgresDsn у рядок для SQLAlchemy
DATABASE_URL = str(settings.database_url)

# Створення асинхронного двигуна з налаштуваннями пулу
engine = create_async_engine(
    DATABASE_URL,
    poolclass=NullPool,
    echo=False,
    connect_args={
        "statement_cache_size": 0,
        "prepared_statement_cache_size": 0,
        "prepared_statement_name_func": lambda: f"__asyncpg_{uuid.uuid4()}__",
    },
)

# Фабрика сесій
async_session = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,  # Запобігає неочікуваним запитам до БД після коміту
    class_=AsyncSession,
)


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Context manager для отримання сесії з автоматичним commit/rollback.
    """
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
