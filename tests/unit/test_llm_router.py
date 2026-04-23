"""
Unit tests for app/clients/llm/router.py and app/clients/llm/gemini_client.py.

Coverage targets:
- clean_json_response(): markdown stripping variants
- LLMRouter.extract_job_data(): openai success, openai fails → gemini fallback,
  both fail → None
- parse_with_gemini(): success, token overflow truncation, retry on ResourceExhausted,
  max retries raises, unexpected exception re-raises
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.api_core.exceptions import ResourceExhausted, ServiceUnavailable

from app.clients.llm.router import LLMRouter, clean_json_response

# ---------------------------------------------------------------------------
# clean_json_response
# ---------------------------------------------------------------------------


class TestCleanJsonResponse:
    """Tests for clean_json_response() helper."""

    def test_plain_json_unchanged(self) -> None:
        raw = '{"title": "Dev"}'
        assert clean_json_response(raw) == '{"title": "Dev"}'

    def test_strips_json_code_fence(self) -> None:
        raw = '```json\n{"title": "Dev"}\n```'
        assert clean_json_response(raw) == '{"title": "Dev"}'

    def test_strips_plain_code_fence(self) -> None:
        raw = '```\n{"title": "Dev"}\n```'
        assert clean_json_response(raw) == '{"title": "Dev"}'

    def test_strips_trailing_fence_only(self) -> None:
        raw = '{"title": "Dev"}\n```'
        assert clean_json_response(raw) == '{"title": "Dev"}'

    def test_strips_leading_whitespace(self) -> None:
        raw = '  \n{"title": "Dev"}'
        assert clean_json_response(raw) == '{"title": "Dev"}'

    def test_empty_string_returns_empty(self) -> None:
        assert clean_json_response("") == ""

    def test_only_fence_returns_empty(self) -> None:
        result = clean_json_response("```json\n```")
        assert result == ""


# ---------------------------------------------------------------------------
# LLMRouter.extract_job_data
# ---------------------------------------------------------------------------


class TestLLMRouterExtractJobData:
    """Tests for LLMRouter.extract_job_data() static method."""

    @pytest.mark.asyncio
    async def test_openai_success_returns_result(self) -> None:
        """OpenAI returns valid JSON → result returned, Gemini not called."""
        with patch(
            "app.clients.llm.router.parse_with_openai",
            new=AsyncMock(return_value='{"title":"Dev"}'),
        ):
            with patch("app.clients.llm.router.parse_with_gemini", new=AsyncMock()) as mock_gemini:
                result = await LLMRouter.extract_job_data("job text")

        assert result == '{"title":"Dev"}'
        mock_gemini.assert_not_called()

    @pytest.mark.asyncio
    async def test_openai_strips_markdown_from_response(self) -> None:
        """OpenAI returns markdown-wrapped JSON → fence stripped before return."""
        raw = '```json\n{"title":"Dev"}\n```'
        with patch("app.clients.llm.router.parse_with_openai", new=AsyncMock(return_value=raw)):
            result = await LLMRouter.extract_job_data("job text")

        assert result == '{"title":"Dev"}'

    @pytest.mark.asyncio
    async def test_openai_fails_falls_back_to_gemini(self) -> None:
        """OpenAI raises → falls back to Gemini and returns its result."""
        with patch(
            "app.clients.llm.router.parse_with_openai",
            new=AsyncMock(side_effect=Exception("openai down")),
        ):
            with patch(
                "app.clients.llm.router.parse_with_gemini",
                new=AsyncMock(return_value='{"title":"Dev"}'),
            ):
                result = await LLMRouter.extract_job_data("job text")

        assert result == '{"title":"Dev"}'

    @pytest.mark.asyncio
    async def test_openai_returns_none_falls_back_to_gemini(self) -> None:
        """OpenAI returns None (falsy) → falls back to Gemini."""
        with patch("app.clients.llm.router.parse_with_openai", new=AsyncMock(return_value=None)):
            with patch(
                "app.clients.llm.router.parse_with_gemini",
                new=AsyncMock(return_value='{"title":"Dev"}'),
            ):
                result = await LLMRouter.extract_job_data("job text")

        assert result == '{"title":"Dev"}'

    @pytest.mark.asyncio
    async def test_both_fail_returns_none(self) -> None:
        """Both OpenAI and Gemini raise → returns None."""
        with patch(
            "app.clients.llm.router.parse_with_openai",
            new=AsyncMock(side_effect=Exception("openai")),
        ):
            with patch(
                "app.clients.llm.router.parse_with_gemini",
                new=AsyncMock(side_effect=Exception("gemini")),
            ):
                result = await LLMRouter.extract_job_data("job text")

        assert result is None

    @pytest.mark.asyncio
    async def test_gemini_returns_none_returns_none(self) -> None:
        """Both return None/falsy → returns None."""
        with patch("app.clients.llm.router.parse_with_openai", new=AsyncMock(return_value=None)):
            with patch(
                "app.clients.llm.router.parse_with_gemini", new=AsyncMock(return_value=None)
            ):
                result = await LLMRouter.extract_job_data("job text")

        assert result is None


# ---------------------------------------------------------------------------
# parse_with_gemini
# ---------------------------------------------------------------------------


class TestParseWithGemini:
    """Tests for parse_with_gemini() function."""

    @pytest.mark.asyncio
    async def test_parse_success_returns_text(self) -> None:
        """Valid response → returns response.text."""
        mock_response = MagicMock()
        mock_response.text = '{"title": "Dev"}'

        mock_token = MagicMock()
        mock_token.total_tokens = 100

        with patch("app.clients.llm.gemini_client.model") as mock_model:
            mock_model.count_tokens.return_value = mock_token
            mock_model.generate_content_async = AsyncMock(return_value=mock_response)

            from app.clients.llm.gemini_client import parse_with_gemini

            result = await parse_with_gemini("short job text")

        assert result == '{"title": "Dev"}'

    @pytest.mark.asyncio
    async def test_parse_truncates_long_text(self) -> None:
        """Text with >8000 tokens → truncated to 30000 chars before sending."""
        mock_response = MagicMock()
        mock_response.text = "{}"

        mock_token = MagicMock()
        mock_token.total_tokens = 9000  # exceeds threshold

        captured_prompts: list[str] = []

        async def capture_prompt(prompt: str) -> MagicMock:
            captured_prompts.append(prompt)
            return mock_response

        with patch("app.clients.llm.gemini_client.model") as mock_model:
            mock_model.count_tokens.return_value = mock_token
            mock_model.generate_content_async = AsyncMock(side_effect=capture_prompt)

            long_text = "x" * 40000
            from app.clients.llm.gemini_client import parse_with_gemini

            await parse_with_gemini(long_text)

        # The prompt contains the truncated text (30000 chars) + EXTRACTION_PROMPT prefix
        assert len(captured_prompts) == 1
        # truncated portion should not exceed 30000 chars inside the prompt
        assert "x" * 30001 not in captured_prompts[0]

    @pytest.mark.asyncio
    async def test_parse_retries_on_resource_exhausted(self) -> None:
        """ResourceExhausted on first call → retries and succeeds on second."""
        mock_response = MagicMock()
        mock_response.text = '{"title": "Dev"}'

        mock_token = MagicMock()
        mock_token.total_tokens = 100

        call_count = 0

        async def side_effect(prompt: str) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ResourceExhausted("quota exceeded")
            return mock_response

        with patch("app.clients.llm.gemini_client.model") as mock_model:
            mock_model.count_tokens.return_value = mock_token
            mock_model.generate_content_async = AsyncMock(side_effect=side_effect)

            from app.clients.llm.gemini_client import parse_with_gemini

            result = await parse_with_gemini("job text")

        assert result == '{"title": "Dev"}'
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_parse_retries_on_service_unavailable(self) -> None:
        """ServiceUnavailable on first call → retries and succeeds."""
        mock_response = MagicMock()
        mock_response.text = "{}"

        mock_token = MagicMock()
        mock_token.total_tokens = 50

        call_count = 0

        async def side_effect(prompt: str) -> MagicMock:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ServiceUnavailable("service down")
            return mock_response

        with patch("app.clients.llm.gemini_client.model") as mock_model:
            mock_model.count_tokens.return_value = mock_token
            mock_model.generate_content_async = AsyncMock(side_effect=side_effect)

            from app.clients.llm.gemini_client import parse_with_gemini

            result = await parse_with_gemini("job text")

        assert result == "{}"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_parse_max_retries_raises(self) -> None:
        """3 consecutive ResourceExhausted → raises after max retries."""
        mock_token = MagicMock()
        mock_token.total_tokens = 50

        with patch("app.clients.llm.gemini_client.model") as mock_model:
            mock_model.count_tokens.return_value = mock_token
            mock_model.generate_content_async = AsyncMock(
                side_effect=ResourceExhausted("quota exceeded")
            )

            from app.clients.llm.gemini_client import parse_with_gemini

            with pytest.raises(ResourceExhausted):
                await parse_with_gemini("job text")

    @pytest.mark.asyncio
    async def test_parse_non_retryable_exception_raises_immediately(self) -> None:
        """ValueError is not retried — raises immediately."""
        mock_token = MagicMock()
        mock_token.total_tokens = 50

        call_count = 0

        async def side_effect(prompt: str) -> None:
            nonlocal call_count
            call_count += 1
            raise ValueError("unexpected")

        with patch("app.clients.llm.gemini_client.model") as mock_model:
            mock_model.count_tokens.return_value = mock_token
            mock_model.generate_content_async = AsyncMock(side_effect=side_effect)

            from app.clients.llm.gemini_client import parse_with_gemini

            with pytest.raises(ValueError):
                await parse_with_gemini("job text")

        assert call_count == 1
