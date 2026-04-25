"""
Unit tests for app/tasks/parse.py — parse_job task.

Coverage targets:
- duplicate URL → early exit before fetching
- serper view fails → status error, client closed
- serper returns empty text → status error
- LLM returns None → status error
- invalid JSON from LLM → status error
- pydantic ValidationError → status error
- filter fails → status filtered
- DB duplicate (upsert returns None) → status duplicate_db
- full happy path → status stored
"""

from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VALID_LLM_JSON = '{"title": "Python Dev", "company": "Acme", "location": "Kyiv"}'
_URL = "https://example.com/job/test"


def _make_fake_redis() -> fakeredis.aioredis.FakeRedis:
    client = fakeredis.aioredis.FakeRedis()
    client.aclose = AsyncMock()  # type: ignore[method-assign]
    return client


def _make_serper(text: str = "job content") -> MagicMock:
    serper = MagicMock()
    serper.view = AsyncMock(return_value=text)
    serper.close = AsyncMock()
    return serper


def _make_repo(job: MagicMock | None = None) -> MagicMock:
    repo = MagicMock()
    repo.upsert = AsyncMock(return_value=job)
    return repo


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestParseJob:
    """Unit tests for parse_job task isolated from DB and real LLM."""

    @pytest.mark.asyncio
    async def test_duplicate_url_returns_duplicate_status(self) -> None:
        """URL already in Redis → returns duplicate without fetching."""
        fake_redis = _make_fake_redis()
        # Pre-seed so second call is duplicate
        from app.services.dedup import DedupService

        await DedupService(fake_redis).is_duplicate(_URL)

        mock_serper = _make_serper()

        mock_context = MagicMock()
        mock_context.state.redis_client = fake_redis

        with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
            from app.tasks.parse import parse_job

            result = await parse_job(_URL, context=mock_context)

        assert result["status"] == "duplicate"
        mock_serper.view.assert_not_called()

    @pytest.mark.asyncio
    async def test_serper_exception_returns_error_and_closes_client(self) -> None:
        """SerperClient.view raises → status error, close() called."""
        fake_redis = _make_fake_redis()
        mock_serper = MagicMock()
        mock_serper.view = AsyncMock(side_effect=Exception("scrape failed"))
        mock_serper.close = AsyncMock()

        mock_context = MagicMock()
        mock_context.state.redis_client = fake_redis

        with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
            from app.tasks.parse import parse_job

            result = await parse_job(_URL, context=mock_context)

        assert result["status"] == "error"
        mock_serper.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_page_returns_error(self) -> None:
        """Serper returns empty string → status error."""
        fake_redis = _make_fake_redis()
        mock_serper = _make_serper(text="")

        mock_context = MagicMock()
        mock_context.state.redis_client = fake_redis

        with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
            from app.tasks.parse import parse_job

            result = await parse_job(_URL, context=mock_context)

        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_llm_returns_none_returns_error(self) -> None:
        """LLMRouter returns None → status error."""
        fake_redis = _make_fake_redis()
        mock_serper = _make_serper()

        mock_context = MagicMock()
        mock_context.state.redis_client = fake_redis

        with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
            with patch("app.tasks.parse.LLMRouter") as mock_router:
                mock_router.extract_job_data = AsyncMock(return_value=None)
                from app.tasks.parse import parse_job

                result = await parse_job(_URL, context=mock_context)

        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_invalid_json_from_llm_returns_error(self) -> None:
        """LLMRouter returns malformed JSON string → status error."""
        fake_redis = _make_fake_redis()
        mock_serper = _make_serper()

        mock_context = MagicMock()
        mock_context.state.redis_client = fake_redis

        with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
            with patch("app.tasks.parse.LLMRouter") as mock_router:
                mock_router.extract_job_data = AsyncMock(return_value="{bad_json")
                from app.tasks.parse import parse_job

                result = await parse_job(_URL, context=mock_context)

        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_pydantic_validation_error_returns_error(self) -> None:
        """LLM JSON missing required fields → ValidationError → status error."""
        fake_redis = _make_fake_redis()
        mock_serper = _make_serper()

        mock_context = MagicMock()
        mock_context.state.redis_client = fake_redis

        # Missing 'title' and 'company' — will fail ParsedJob validation
        invalid_json = '{"location": "Kyiv"}'

        with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
            with patch("app.tasks.parse.LLMRouter") as mock_router:
                mock_router.extract_job_data = AsyncMock(return_value=invalid_json)
                from app.tasks.parse import parse_job

                result = await parse_job(_URL, context=mock_context)

        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_filter_fails_returns_filtered(self) -> None:
        """FilterEngine.passes returns False → status filtered."""
        fake_redis = _make_fake_redis()
        mock_serper = _make_serper()

        mock_context = MagicMock()
        mock_context.state.redis_client = fake_redis

        with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
            with patch("app.tasks.parse.LLMRouter") as mock_router:
                mock_router.extract_job_data = AsyncMock(return_value=VALID_LLM_JSON)
                with patch("app.tasks.parse.FilterEngine") as mock_filter_cls:
                    mock_filter = MagicMock()
                    mock_filter.passes.return_value = False
                    mock_filter_cls.return_value = mock_filter

                    from app.tasks.parse import parse_job

                    result = await parse_job(_URL, context=mock_context)

        assert result["status"] == "filtered"

    @pytest.mark.asyncio
    async def test_db_duplicate_returns_duplicate_db(self) -> None:
        """upsert returns None (DB conflict) → status duplicate_db."""
        fake_redis = _make_fake_redis()
        mock_repo = _make_repo(job=None)

        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_context = MagicMock()
        mock_context.state.redis_client = fake_redis
        mock_serper = _make_serper()

        with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
            with patch("app.tasks.parse.LLMRouter") as mock_router:
                mock_router.extract_job_data = AsyncMock(return_value=VALID_LLM_JSON)
                with patch("app.tasks.parse.FilterEngine") as mock_filter_cls:
                    mock_filter = MagicMock()
                    mock_filter.passes.return_value = True
                    mock_filter_cls.return_value = mock_filter
                    with patch("app.tasks.parse.get_session", return_value=mock_session):
                        with patch("app.tasks.parse.JobRepository", return_value=mock_repo):
                            from app.tasks.parse import parse_job

                            result = await parse_job(_URL, context=mock_context)

        assert result["status"] == "duplicate_db"

    @pytest.mark.asyncio
    async def test_happy_path_returns_stored_with_job_id(self) -> None:
        """Full pipeline success → status stored, job_id set."""
        fake_redis = _make_fake_redis()

        mock_job = MagicMock()
        mock_job.id = 99
        mock_repo = _make_repo(job=mock_job)

        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_context = MagicMock()
        mock_context.state.redis_client = fake_redis
        mock_serper = _make_serper()

        with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
            with patch("app.tasks.parse.LLMRouter") as mock_router:
                mock_router.extract_job_data = AsyncMock(return_value=VALID_LLM_JSON)
                with patch("app.tasks.parse.FilterEngine") as mock_filter_cls:
                    mock_filter = MagicMock()
                    mock_filter.passes.return_value = True
                    mock_filter_cls.return_value = mock_filter
                    with patch("app.tasks.parse.get_session", return_value=mock_session):
                        with patch("app.tasks.parse.JobRepository", return_value=mock_repo):
                            from app.tasks.parse import parse_job

                            result = await parse_job(_URL, context=mock_context)

        assert result["status"] == "stored"
        assert result["job_id"] == 99

    @pytest.mark.asyncio
    async def test_serper_client_closed_after_success(self) -> None:
        """serper.close() always called after view, even on success."""
        fake_redis = _make_fake_redis()
        mock_serper = _make_serper()

        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_job = MagicMock()
        mock_job.id = 1
        mock_repo = _make_repo(job=mock_job)

        mock_context = MagicMock()
        mock_context.state.redis_client = fake_redis

        with patch("app.tasks.parse.SerperClient", return_value=mock_serper):
            with patch("app.tasks.parse.LLMRouter") as mock_router:
                mock_router.extract_job_data = AsyncMock(return_value=VALID_LLM_JSON)
                with patch("app.tasks.parse.FilterEngine") as mock_filter_cls:
                    mock_filter = MagicMock()
                    mock_filter.passes.return_value = True
                    mock_filter_cls.return_value = mock_filter
                    with patch("app.tasks.parse.get_session", return_value=mock_session):
                        with patch("app.tasks.parse.JobRepository", return_value=mock_repo):
                            from app.tasks.parse import parse_job

                            await parse_job(_URL, context=mock_context)

        mock_serper.close.assert_called_once()
