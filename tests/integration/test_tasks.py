"""
Integration tests for app/tasks/scrape.py and app/tasks/parse.py.

Strategy:
- Real fakeredis for DedupService
- Real PostgreSQL via testcontainers
- External clients (SerperClient, LLMRouter) are mocked at module boundaries
- TaskIQ broker is set to in-memory mode via broker.is_worker_process = True workaround:
  tasks are called directly (not via .kiq()) to avoid needing a running worker

Run: pytest tests/integration/test_tasks.py -v
"""

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from testcontainers.postgres import PostgresContainer  # type: ignore[import-untyped]

from app.models.job import Base

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")  # pyright: ignore[reportUntypedFunctionDecorator]
def postgres_container():  # type: ignore[no-untyped-def]
    """Start PostgreSQL container for the entire test module."""
    with PostgresContainer("postgres:15-alpine") as pg:
        yield pg


@pytest_asyncio.fixture(scope="module")  # pyright: ignore[reportUntypedFunctionDecorator]
async def db_engine(postgres_container: PostgresContainer):  # type: ignore[no-untyped-def]
    """Async SQLAlchemy engine connected to testcontainers."""
    # Use psycopg2 driver URL format mapped to asyncpg
    url = postgres_container.get_connection_url().replace("psycopg2", "asyncpg")
    engine = create_async_engine(url, poolclass=NullPool)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    yield engine
    await engine.dispose()


@pytest_asyncio.fixture  # pyright: ignore[reportUntypedFunctionDecorator]
async def db_session(db_engine) -> AsyncGenerator[AsyncSession, None]:  # type: ignore[no-untyped-def]
    """Isolated DB session for each test."""
    SessionLocal = async_sessionmaker(db_engine, expire_on_commit=False)
    async with SessionLocal() as session:
        yield session
        # Rollback uncommitted changes to keep tests isolated
        await session.rollback()


@pytest_asyncio.fixture  # pyright: ignore[reportUntypedFunctionDecorator]
async def fake_redis() -> AsyncGenerator[fakeredis.aioredis.FakeRedis, None]:
    """FakeRedis instance, flushed before each test."""
    client = fakeredis.aioredis.FakeRedis()
    await client.flushall()
    yield client
    await client.aclose()  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Tests: scrape.py
# ---------------------------------------------------------------------------


class TestScrapeJobPageTask:
    """Integration tests for scrape_job_page."""

    @pytest.mark.asyncio
    async def test_scrape_success_queues_parse_tasks(self) -> None:
        """Valid search returns URLs → queues parse_job tasks."""
        mock_serper = MagicMock()
        # Mock search returning two valid job items
        mock_serper.search = AsyncMock(
            return_value=[
                {"url": "https://example.com/job1"},
                {"url": "https://example.com/job2"},
            ]
        )

        with patch("app.tasks.scrape.SerperClient", return_value=mock_serper):
            with patch("app.tasks.scrape.parse_job") as mock_parse_job:
                # Mock the taskiq .kiq() call
                mock_kiq = AsyncMock()
                mock_parse_job.kiq = mock_kiq

                from app.tasks.scrape import scrape_job_page

                result = await scrape_job_page("Python Developer")

        assert result["urls_found"] == 2
        assert result["tasks_queued"] == 2
        assert mock_kiq.call_count == 2
        mock_kiq.assert_any_call("https://example.com/job1")
        mock_kiq.assert_any_call("https://example.com/job2")

    @pytest.mark.asyncio
    async def test_scrape_no_results_returns_zeros(self) -> None:
        """Search returns empty list → 0 queued."""
        mock_serper = MagicMock()
        mock_serper.search = AsyncMock(return_value=[])

        with patch("app.tasks.scrape.SerperClient", return_value=mock_serper):
            with patch("app.tasks.scrape.parse_job") as mock_parse_job:
                mock_parse_job.kiq = AsyncMock()
                from app.tasks.scrape import scrape_job_page

                result = await scrape_job_page("Non Existent Job 123")

        assert result["urls_found"] == 0
        assert result["tasks_queued"] == 0
        mock_parse_job.kiq.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: parse.py
# ---------------------------------------------------------------------------


class TestParseJobPipeline:
    """Integration tests for parse_job using real DB & Redis."""

    @pytest.mark.asyncio
    async def test_parse_job_full_pipeline_stores_job(
        self, db_session: AsyncSession, fake_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """Full happy path: dedup miss → fetch → parse → filter pass → stored in DB."""
        url = "https://example.com/job/parse-full"

        mock_serper = MagicMock()
        mock_serper.view = AsyncMock(return_value="Python developer at Acme in Kyiv.")
        mock_serper.close = AsyncMock()

        mock_context = MagicMock()
        mock_context.state.redis_client = fake_redis

        with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
            with patch("app.tasks.parse.LLMRouter") as mock_router:
                # LLM successfully extracts JSON
                mock_router.extract_job_data = AsyncMock(
                    return_value='{"title": "Python Dev", "company": "Acme", "location": "Kyiv"}'
                )
                # Ensure get_session yields our test DB session
                with patch("app.tasks.parse.get_session") as mock_get_session:
                    mock_session_ctx = MagicMock()
                    mock_session_ctx.__aenter__ = AsyncMock(return_value=db_session)
                    mock_session_ctx.__aexit__ = AsyncMock(return_value=False)
                    mock_get_session.return_value = mock_session_ctx

                    from app.tasks.parse import parse_job

                    result = await parse_job(url, context=mock_context)

        # Assert pipeline succeeded
        assert result["status"] == "stored"
        assert result["job_id"] is not None

        # Verify it's actually in the database
        from sqlalchemy import select

        from app.models.job import Job

        db_result = await db_session.execute(select(Job).where(Job.id == result["job_id"]))
        stored_job = db_result.scalar_one_or_none()

        assert stored_job is not None
        assert stored_job.title == "Python Dev"
        assert stored_job.company == "Acme"
        assert stored_job.source_url == url

        # Verify it's marked as duplicate in Redis now
        from app.services.dedup import DedupService

        dedup = DedupService(fake_redis)
        assert await dedup.is_duplicate(url) is True

    @pytest.mark.asyncio
    async def test_parse_job_duplicate_early_exit(
        self, fake_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """URL already in Redis → returns duplicate status without fetching page."""
        url = "https://example.com/job/already-seen"

        # Pre-seed the key so DedupService sees it as duplicate
        from app.services.dedup import DedupService

        dedup = DedupService(fake_redis)
        await dedup.is_duplicate(url)  # first call sets the key

        mock_context = MagicMock()
        mock_context.state.redis_client = fake_redis

        with patch("app.tasks.parse.SerperClient") as mock_serper_cls:
            from app.tasks.parse import parse_job

            result = await parse_job(url, context=mock_context)

        assert result["status"] == "duplicate"
        mock_serper_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_parse_job_filtered_skip(self, fake_redis: fakeredis.aioredis.FakeRedis) -> None:
        """Job fails FilterEngine → status 'filtered', not stored in DB."""
        url = "https://example.com/job/filtered"

        mock_serper = MagicMock()
        mock_serper.view = AsyncMock(return_value="Some job text")
        mock_serper.close = AsyncMock()

        mock_context = MagicMock()
        mock_context.state.redis_client = fake_redis

        with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
            with patch("app.tasks.parse.LLMRouter") as mock_router:
                # Return JSON that will fail filter (e.g., wrong location)
                mock_router.extract_job_data = AsyncMock(
                    return_value='{"title": "Java Dev", "company": "Acme", "location": "London"}'
                )
                # Override config temporarily
                with patch("app.tasks.parse.get_settings") as mock_settings:
                    settings = MagicMock()
                    settings.filter_keywords = ["Python"]
                    settings.filter_location = ["Kyiv"]
                    settings.filter_salary_min = 0
                    settings.dedup_ttl_seconds = 3600
                    mock_settings.return_value = settings

                    from app.tasks.parse import parse_job

                    result = await parse_job(url, context=mock_context)

        assert result["status"] == "filtered"

    @pytest.mark.asyncio
    async def test_parse_job_empty_page_content_returns_error(
        self, fake_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """Serper returns empty text → status 'error' without calling LLM."""
        url = "https://example.com/job/empty-page"

        mock_serper = MagicMock()
        mock_serper.view = AsyncMock(return_value="")
        mock_serper.close = AsyncMock()

        mock_context = MagicMock()
        mock_context.state.redis_client = fake_redis

        with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
            with patch("app.tasks.parse.LLMRouter") as mock_router:
                from app.tasks.parse import parse_job

                result = await parse_job(url, context=mock_context)

        assert result["status"] == "error"
        mock_router.extract_job_data.assert_not_called()

    @pytest.mark.asyncio
    async def test_parse_job_llm_failure_returns_error(
        self, fake_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """LLMRouter returns None → status 'error'."""
        url = "https://example.com/job/llm-fail"

        mock_serper = MagicMock()
        mock_serper.view = AsyncMock(return_value="Some job content")
        mock_serper.close = AsyncMock()

        mock_context = MagicMock()
        mock_context.state.redis_client = fake_redis

        with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
            with patch("app.tasks.parse.LLMRouter") as mock_router:
                mock_router.extract_job_data = AsyncMock(return_value=None)
                from app.tasks.parse import parse_job

                result = await parse_job(url, context=mock_context)

        assert result["status"] == "error"
