import hashlib
from typing import Optional, Sequence

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import Job
from app.schemas.job import ParsedJob


class JobRepository:
    def __init__(self, session: AsyncSession):
        """Ініціалізація з ін'єкцією сесії бази даних."""
        self.session = session

    def _generate_external_id(self, url: str) -> str:
        """Генерація унікального ідентифікатора на основі URL."""
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    async def upsert(self, job_data: ParsedJob) -> Optional[Job]:
        """
        Вставка нової вакансії або ігнорування, якщо external_id вже існує.
        Повертає об'єкт Job при успішній вставці, або None при конфлікті.
        """
        external_id = self._generate_external_id(job_data.url)

        description_snippet = None
        if job_data.description:
            description_snippet = job_data.description[:500]

        stmt = insert(Job).values(
            external_id=external_id,
            title=job_data.title,
            company=job_data.company,
            location=job_data.location,
            salary_min=job_data.salary_min,
            salary_max=job_data.salary_max,
            salary_currency="USD",
            skills=job_data.skills,
            description_snippet=description_snippet,
            source_url=job_data.url,
        )

        # PostgreSQL ON CONFLICT
        stmt = stmt.on_conflict_do_nothing(index_elements=["external_id"]).returning(Job)

        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_unnotified(self, limit: int = 50) -> Sequence[Job]:
        """Отримання списку вакансій, які ще не відправлені у Slack."""
        stmt = (
            select(Job).where(Job.notified.is_(False)).order_by(Job.created_at.asc()).limit(limit)
        )
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def mark_notified(self, job_id: int) -> None:
        """Позначення вакансії як відправленої."""
        stmt = update(Job).where(Job.id == job_id).values(notified=True)
        await self.session.execute(stmt)

    async def get_by_external_id(self, external_id: str) -> Optional[Job]:
        """Пошук вакансії за external_id."""
        stmt = select(Job).where(Job.external_id == external_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
