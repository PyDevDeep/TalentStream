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
    """Async engine with schema created."""
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
async def db_session(db_engine) -> AsyncGenerator[AsyncSession, None]:  # type: ignore[no-untyped-def]
    """Per-test session, rolled back after each test."""
    factory = async_sessionmaker(bind=db_engine, expire_on_commit=False, class_=AsyncSession)
    async with factory() as sess:
        yield sess
        await sess.rollback()


@pytest_asyncio.fixture  # pyright: ignore[reportUntypedFunctionDecorator]
async def fake_redis() -> AsyncGenerator[fakeredis.aioredis.FakeRedis, None]:
    """In-memory Redis for dedup and broker."""
    client = fakeredis.aioredis.FakeRedis()
    yield client
    await client.aclose()


# ---------------------------------------------------------------------------
# scrape_job_page tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestScrapeJobPage:
    """Tests for scrape_job_page task logic (called directly)."""

    @pytest.mark.asyncio
    async def test_scrape_job_page_queues_tasks(self) -> None:
        """Serper search returns 2 URLs → parse_job.kiq called twice."""
        urls = ["https://example.com/job/1", "https://example.com/job/2"]

        mock_serper = MagicMock()
        mock_serper.search = AsyncMock(return_value=urls)
        mock_serper.close = AsyncMock()

        mock_kiq = AsyncMock()

        with patch("app.tasks.scrape.SerperClient", return_value=mock_serper):
            with patch("app.tasks.scrape.parse_job") as mock_parse_job:
                mock_parse_job.kiq = mock_kiq
                from app.tasks.scrape import scrape_job_page

                result = await scrape_job_page("Python Developer")

        assert result["urls_found"] == 2
        assert result["tasks_queued"] == 2
        assert mock_kiq.call_count == 2

    @pytest.mark.asyncio
    async def test_scrape_job_page_empty_results(self) -> None:
        """Serper returns 0 URLs → no tasks queued."""
        mock_serper = MagicMock()
        mock_serper.search = AsyncMock(return_value=[])
        mock_serper.close = AsyncMock()

        with patch("app.tasks.scrape.SerperClient", return_value=mock_serper):
            with patch("app.tasks.scrape.parse_job") as mock_parse_job:
                mock_parse_job.kiq = AsyncMock()
                from app.tasks.scrape import scrape_job_page

                result = await scrape_job_page("Unknown Query")

        assert result["urls_found"] == 0
        assert result["tasks_queued"] == 0
        mock_parse_job.kiq.assert_not_called()

    @pytest.mark.asyncio
    async def test_scrape_job_page_closes_client_on_error(self) -> None:
        """SerperClient.close() is called even when search raises an exception."""
        mock_serper = MagicMock()
        mock_serper.search = AsyncMock(side_effect=Exception("API down"))
        mock_serper.close = AsyncMock()

        with patch("app.tasks.scrape.SerperClient", return_value=mock_serper):
            from app.tasks.scrape import scrape_job_page

            with pytest.raises(Exception, match="API down"):
                await scrape_job_page("Python Developer")

        mock_serper.close.assert_called_once()


# ---------------------------------------------------------------------------
# parse_job tests
# ---------------------------------------------------------------------------


VALID_LLM_JSON = '{"title": "Python Dev", "company": "Acme", "location": "Kyiv"}'


@pytest.mark.integration
class TestParseJobPipeline:
    """Tests for parse_job task logic (called directly)."""

    @pytest.mark.asyncio
    async def test_parse_job_full_pipeline_stores_job(
        self, db_session: AsyncSession, fake_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """Full happy path: dedup miss → fetch → parse → filter pass → stored in DB."""
        url = "https://example.com/job/parse-full"

        mock_serper = MagicMock()
        mock_serper.view = AsyncMock(return_value="Python developer at Acme in Kyiv.")
        mock_serper.close = AsyncMock()

        with patch("app.tasks.parse.Redis") as mock_redis_cls:
            mock_redis_cls.from_url.return_value = fake_redis
            # Prevent double-close of shared fake_redis
            fake_redis.aclose = AsyncMock()

            with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
                with patch("app.tasks.parse.LLMRouter") as mock_router:
                    mock_router.extract_job_data = AsyncMock(return_value=VALID_LLM_JSON)

                    with patch("app.tasks.parse.get_session") as mock_get_session:
                        mock_get_session.return_value.__aenter__ = AsyncMock(
                            return_value=db_session
                        )
                        mock_get_session.return_value.__aexit__ = AsyncMock(return_value=False)

                        with patch("app.tasks.parse.FilterEngine") as mock_filter_cls:
                            mock_filter = MagicMock()
                            mock_filter.passes.return_value = True
                            mock_filter_cls.return_value = mock_filter

                            from app.tasks.parse import parse_job

                            result = await parse_job(url)

        assert result["status"] == "stored"
        assert result["job_id"] is not None

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

        with patch("app.tasks.parse.Redis") as mock_redis_cls:
            mock_redis_cls.from_url.return_value = fake_redis
            fake_redis.aclose = AsyncMock()

            mock_serper = MagicMock()
            mock_serper.view = AsyncMock()

            with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
                from app.tasks.parse import parse_job

                result = await parse_job(url)

        assert result["status"] == "duplicate"
        mock_serper.view.assert_not_called()

    @pytest.mark.asyncio
    async def test_parse_job_filtered_skip(self, fake_redis: fakeredis.aioredis.FakeRedis) -> None:
        """Job fails FilterEngine → status 'filtered', not stored in DB."""
        url = "https://example.com/job/filtered"

        mock_serper = MagicMock()
        mock_serper.view = AsyncMock(return_value="Some job text")
        mock_serper.close = AsyncMock()

        with patch("app.tasks.parse.Redis") as mock_redis_cls:
            mock_redis_cls.from_url.return_value = fake_redis
            fake_redis.aclose = AsyncMock()

            with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
                with patch("app.tasks.parse.LLMRouter") as mock_router:
                    mock_router.extract_job_data = AsyncMock(return_value=VALID_LLM_JSON)

                    with patch("app.tasks.parse.FilterEngine") as mock_filter_cls:
                        mock_filter = MagicMock()
                        mock_filter.passes.return_value = False
                        mock_filter_cls.return_value = mock_filter

                        from app.tasks.parse import parse_job

                        result = await parse_job(url)

        assert result["status"] == "filtered"
        assert result["job_id"] is None

    @pytest.mark.asyncio
    async def test_parse_job_empty_page_content_returns_error(
        self, fake_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """Serper returns empty text → status 'error' without calling LLM."""
        url = "https://example.com/job/empty-page"

        mock_serper = MagicMock()
        mock_serper.view = AsyncMock(return_value="")
        mock_serper.close = AsyncMock()

        with patch("app.tasks.parse.Redis") as mock_redis_cls:
            mock_redis_cls.from_url.return_value = fake_redis
            fake_redis.aclose = AsyncMock()

            with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
                with patch("app.tasks.parse.LLMRouter") as mock_router:
                    from app.tasks.parse import parse_job

                    result = await parse_job(url)

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

        with patch("app.tasks.parse.Redis") as mock_redis_cls:
            mock_redis_cls.from_url.return_value = fake_redis
            fake_redis.aclose = AsyncMock()

            with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
                with patch("app.tasks.parse.LLMRouter") as mock_router:
                    mock_router.extract_job_data = AsyncMock(return_value=None)

                    from app.tasks.parse import parse_job

                    result = await parse_job(url)

        assert result["status"] == "error"
