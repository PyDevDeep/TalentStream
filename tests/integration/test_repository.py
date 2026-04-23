"""
Integration tests for app/db/repository.py.

Uses testcontainers to spin up a real PostgreSQL instance.
All tests run against actual DB — no mocking of SQL layer.

Run: pytest tests/integration/test_repository.py -v
"""

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

from app.db.repository import JobRepository
from app.models.job import Base, Job
from app.schemas.job import ParsedJob

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")  # pyright: ignore[reportUntypedFunctionDecorator]
def postgres_container():  # type: ignore[no-untyped-def]
    """Start a real PostgreSQL container for the entire test module."""
    with PostgresContainer("postgres:15-alpine") as pg:
        yield pg


@pytest_asyncio.fixture(scope="module")  # pyright: ignore[reportUntypedFunctionDecorator]
async def db_engine(postgres_container: PostgresContainer):  # type: ignore[no-untyped-def]
    """Create async engine pointed at the test container and run migrations."""
    # testcontainers returns a sync DSN — convert to asyncpg driver
    sync_url: str = postgres_container.get_connection_url()
    async_url = sync_url.replace("postgresql+psycopg2://", "postgresql+asyncpg://").replace(
        "postgresql://", "postgresql+asyncpg://"
    )

    engine = create_async_engine(async_url, poolclass=NullPool, echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest_asyncio.fixture  # pyright: ignore[reportUntypedFunctionDecorator]
async def session(db_engine) -> AsyncGenerator[AsyncSession, None]:  # type: ignore[no-untyped-def]
    """Provide a fresh session per test, rolled back after each test."""
    factory = async_sessionmaker(bind=db_engine, expire_on_commit=False, class_=AsyncSession)
    async with factory() as sess:
        yield sess
        await sess.rollback()


@pytest.fixture
def sample_job_data() -> ParsedJob:
    """Minimal valid ParsedJob for upsert testing."""
    return ParsedJob(
        title="Python Developer",
        company="Acme Corp",
        url="https://example.com/job/100",
        location="Kyiv, Ukraine",
        skills=["Python", "FastAPI"],
        salary_min=4000,
        salary_max=6000,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestJobRepositoryUpsert:
    """Tests for JobRepository.upsert()."""

    @pytest.mark.asyncio
    async def test_upsert_new_job_returns_job_instance(
        self, session: AsyncSession, sample_job_data: ParsedJob
    ) -> None:
        """Inserting a new job → returns a Job ORM instance."""
        repo = JobRepository(session)
        job = await repo.upsert(sample_job_data)

        assert job is not None
        assert isinstance(job, Job)
        assert job.title == "Python Developer"
        assert job.company == "Acme Corp"

    @pytest.mark.asyncio
    async def test_upsert_new_job_sets_external_id(
        self, session: AsyncSession, sample_job_data: ParsedJob
    ) -> None:
        """Upserted job has external_id derived from URL hash."""
        import hashlib

        repo = JobRepository(session)
        job = await repo.upsert(sample_job_data)

        expected_id = hashlib.sha256(sample_job_data.url.encode()).hexdigest()
        assert job is not None
        assert job.external_id == expected_id

    @pytest.mark.asyncio
    async def test_upsert_duplicate_returns_none(self, session: AsyncSession) -> None:
        """Inserting the same URL twice → second call returns None (conflict)."""
        repo = JobRepository(session)
        job_data = ParsedJob(
            title="Duplicate Job",
            company="Corp",
            url="https://example.com/job/duplicate",
        )

        first = await repo.upsert(job_data)
        await session.flush()
        second = await repo.upsert(job_data)

        assert first is not None
        assert second is None

    @pytest.mark.asyncio
    async def test_upsert_truncates_description_to_500_chars(self, session: AsyncSession) -> None:
        """Long description is stored as a 500-char snippet."""
        long_desc = "x" * 1000
        job_data = ParsedJob(
            title="Desc Test Job",
            company="Corp",
            url="https://example.com/job/desc",
            description=long_desc,
        )
        repo = JobRepository(session)
        job = await repo.upsert(job_data)

        assert job is not None
        assert job.description_snippet is not None
        assert len(job.description_snippet) == 500

    @pytest.mark.asyncio
    async def test_upsert_none_description_stored_as_none(self, session: AsyncSession) -> None:
        """Job without description → description_snippet is None."""
        job_data = ParsedJob(
            title="No Desc Job",
            company="Corp",
            url="https://example.com/job/nodesc",
            description=None,
        )
        repo = JobRepository(session)
        job = await repo.upsert(job_data)

        assert job is not None
        assert job.description_snippet is None


@pytest.mark.integration
class TestJobRepositoryGetUnnotified:
    """Tests for JobRepository.get_unnotified()."""

    @pytest.mark.asyncio
    async def test_get_unnotified_returns_correct_jobs(self, session: AsyncSession) -> None:
        """Returns only jobs where notified=False."""
        repo = JobRepository(session)

        job1 = await repo.upsert(
            ParsedJob(title="Job A", company="Corp", url="https://example.com/unnotified/1")
        )
        job2 = await repo.upsert(
            ParsedJob(title="Job B", company="Corp", url="https://example.com/unnotified/2")
        )
        await session.flush()

        assert job1 is not None
        assert job2 is not None

        unnotified = await repo.get_unnotified(limit=50)
        ids = [j.id for j in unnotified]
        assert job1.id in ids
        assert job2.id in ids

    @pytest.mark.asyncio
    async def test_get_unnotified_respects_limit(self, session: AsyncSession) -> None:
        """get_unnotified(limit=1) returns at most 1 job."""
        repo = JobRepository(session)

        for i in range(3):
            await repo.upsert(
                ParsedJob(
                    title=f"Limit Job {i}",
                    company="Corp",
                    url=f"https://example.com/limit/{i}",
                )
            )
        await session.flush()

        result = await repo.get_unnotified(limit=1)
        assert len(result) <= 1


@pytest.mark.integration
class TestJobRepositoryMarkNotified:
    """Tests for JobRepository.mark_notified()."""

    @pytest.mark.asyncio
    async def test_mark_notified_updates_flag(self, session: AsyncSession) -> None:
        """After mark_notified(), job.notified becomes True."""
        repo = JobRepository(session)
        job = await repo.upsert(
            ParsedJob(
                title="Notify Me",
                company="Corp",
                url="https://example.com/notify/1",
            )
        )
        await session.flush()
        assert job is not None

        await repo.mark_notified(job.id)
        await session.flush()

        updated = await repo.get_by_external_id(job.external_id)
        assert updated is not None
        assert updated.notified is True

    @pytest.mark.asyncio
    async def test_marked_job_excluded_from_unnotified(self, session: AsyncSession) -> None:
        """Job marked as notified does not appear in get_unnotified()."""
        repo = JobRepository(session)
        job = await repo.upsert(
            ParsedJob(
                title="Already Notified",
                company="Corp",
                url="https://example.com/notify/2",
            )
        )
        await session.flush()
        assert job is not None

        await repo.mark_notified(job.id)
        await session.flush()

        unnotified = await repo.get_unnotified(limit=50)
        ids = [j.id for j in unnotified]
        assert job.id not in ids
