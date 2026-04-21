from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings

settings = get_settings()

# Перетворюємо PostgresDsn у рядок для SQLAlchemy
DATABASE_URL = str(settings.database_url)

# Створення асинхронного двигуна з налаштуваннями пулу
engine = create_async_engine(
    DATABASE_URL,
    pool_size=10,  # Базовий розмір пулу
    max_overflow=20,  # Максимальна кількість додаткових з'єднань
    pool_timeout=30,  # Таймаут очікування з'єднання
    pool_recycle=1800,  # Оновлення з'єднань кожні 30 хв
    pool_pre_ping=True,  # Перевірка життєздатності з'єднання перед використанням
    echo=False,  # Встанови True для дебагу SQL запитів
    connect_args={"statement_cache_size": 0},  # FIX: Вимкнення кешування для Supabase/PgBouncer
)

# Фабрика сесій
async_session = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,  # Запобігає неочікуваним запитам до БД після коміту
    class_=AsyncSession,
)


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
