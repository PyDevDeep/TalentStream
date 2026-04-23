import hashlib
from typing import Optional, Sequence

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import Job
from app.schemas.job import ParsedJob


class JobRepository:
    def __init__(self, session: AsyncSession):
        """Initialize with an injected database session."""
        self.session = session

    def _generate_external_id(self, url: str) -> str:
        """Generate a unique identifier from the job URL using SHA-256."""
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    async def upsert(self, job_data: ParsedJob) -> Optional[Job]:
        """Insert a new job or ignore if external_id already exists. Returns the Job on insert, None on conflict."""
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

        stmt = stmt.on_conflict_do_nothing(index_elements=["external_id"]).returning(Job)

        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_unnotified(self, limit: int = 50) -> Sequence[Job]:
        """Return jobs that have not yet been sent to Slack, ordered by creation time."""
        stmt = (
            select(Job).where(Job.notified.is_(False)).order_by(Job.created_at.asc()).limit(limit)
        )
        result = await self.session.execute(stmt)
        return result.scalars().all()

    async def mark_notified(self, job_id: int) -> None:
        """Mark a job as notified in the database."""
        stmt = update(Job).where(Job.id == job_id).values(notified=True)
        await self.session.execute(stmt)

    async def get_by_external_id(self, external_id: str) -> Optional[Job]:
        """Look up a job by its external_id."""
        stmt = select(Job).where(Job.external_id == external_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
