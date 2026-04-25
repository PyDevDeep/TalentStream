"""
Unit tests for app/clients/llm/router.py and app/clients/llm/gemini_client.py.
"""

from typing import Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.api_core.exceptions import ResourceExhausted

from app.clients.llm.base import LLMClientProtocol
from app.clients.llm.gemini_client import GeminiClient
from app.clients.llm.router import LLMRouter

# ---------------------------------------------------------------------------
# LLMRouter (Testing Dependency Injection)
# ---------------------------------------------------------------------------


class TestLLMRouter:
    """Tests for LLMRouter using Mock injected clients."""

    @pytest.fixture
    def mock_primary(self) -> AsyncMock:
        m = AsyncMock(spec=LLMClientProtocol)
        m.parse.return_value = '{"title":"Dev"}'
        return m

    @pytest.fixture
    def mock_fallback(self) -> AsyncMock:
        m = AsyncMock(spec=LLMClientProtocol)
        m.parse.return_value = '{"title":"Fallback Dev"}'
        return m

    @pytest.mark.asyncio
    async def test_primary_success_returns_result(
        self, mock_primary: AsyncMock, mock_fallback: AsyncMock
    ) -> None:
        """Primary returns valid JSON → result returned, fallback not called."""
        router = LLMRouter(primary_client=mock_primary, fallback_client=mock_fallback)
        result = await router.extract_job_data("job text")

        assert result == '{"title":"Dev"}'
        mock_primary.parse.assert_called_once_with("job text")
        mock_fallback.parse.assert_not_called()

    @pytest.mark.asyncio
    async def test_primary_fails_falls_back(
        self, mock_primary: AsyncMock, mock_fallback: AsyncMock
    ) -> None:
        """Primary raises Exception → falls back to fallback client."""
        mock_primary.parse.side_effect = Exception("openai down")

        router = LLMRouter(primary_client=mock_primary, fallback_client=mock_fallback)
        result = await router.extract_job_data("job text")

        assert result == '{"title":"Fallback Dev"}'
        mock_fallback.parse.assert_called_once_with("job text")

    @pytest.mark.asyncio
    async def test_both_fail_returns_none(
        self, mock_primary: AsyncMock, mock_fallback: AsyncMock
    ) -> None:
        """Both clients raise Exception → returns None."""
        mock_primary.parse.side_effect = Exception("openai down")
        mock_fallback.parse.side_effect = Exception("gemini down")

        router = LLMRouter(primary_client=mock_primary, fallback_client=mock_fallback)
        result = await router.extract_job_data("job text")

        assert result is None


# ---------------------------------------------------------------------------
# GeminiClient (Testing Retry logic and Truncation)
# ---------------------------------------------------------------------------


class TestGeminiClient:
    """Tests for GeminiClient class."""

    @pytest.fixture
    def mock_genai(self) -> Generator[MagicMock, None, None]:
        with patch("app.clients.llm.gemini_client.genai") as mock:
            yield mock

    @pytest.mark.asyncio
    async def test_parse_success_returns_text(self, mock_genai: MagicMock) -> None:
        """Valid response → returns response.text."""
        mock_response = MagicMock()
        mock_response.text = '{"title": "Dev"}'

        mock_model = MagicMock()
        mock_model.generate_content_async = AsyncMock(return_value=mock_response)
        mock_genai.GenerativeModel.return_value = mock_model

        client = GeminiClient(api_key="fake_key")
        result = await client.parse("short job text")

        assert result == '{"title": "Dev"}'
        mock_model.generate_content_async.assert_called_once()

    @pytest.mark.asyncio
    async def test_parse_truncates_long_text(self, mock_genai: MagicMock) -> None:
        """Text exceeding max chars is truncated before sending to model."""
        mock_response = MagicMock()
        mock_response.text = "{}"

        mock_model = MagicMock()
        mock_model.generate_content_async = AsyncMock(return_value=mock_response)
        mock_genai.GenerativeModel.return_value = mock_model

        client = GeminiClient(api_key="fake_key")
        client._max_text_chars = 10

        await client.parse("1234567890_THIS_SHOULD_BE_CUT")

        call_args = mock_model.generate_content_async.call_args[0][0]
        assert "1234567890" in call_args
        assert "_THIS_SHOULD_BE_CUT" not in call_args

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_parse_retries_on_resource_exhausted(
        self, mock_sleep: AsyncMock, mock_genai: MagicMock
    ) -> None:
        """ResourceExhausted on first call → retries and succeeds."""
        mock_response = MagicMock()
        mock_response.text = '{"title": "Dev"}'

        mock_model = MagicMock()
        mock_model.generate_content_async = AsyncMock(
            side_effect=[ResourceExhausted("quota exceeded"), mock_response]
        )
        mock_genai.GenerativeModel.return_value = mock_model

        client = GeminiClient(api_key="fake_key")
        result = await client.parse("job text")

        assert result == '{"title": "Dev"}'
        assert mock_model.generate_content_async.call_count == 2

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_parse_max_retries_raises(
        self, mock_sleep: AsyncMock, mock_genai: MagicMock
    ) -> None:
        """Consecutive ResourceExhausted → raises exception."""
        mock_model = MagicMock()
        mock_model.generate_content_async = AsyncMock(
            side_effect=ResourceExhausted("quota exceeded")
        )
        mock_genai.GenerativeModel.return_value = mock_model

        client = GeminiClient(api_key="fake_key")

        with pytest.raises(ResourceExhausted):
            await client.parse("job text")

        assert mock_model.generate_content_async.call_count == 3

    @pytest.mark.asyncio
    async def test_parse_non_retryable_exception_raises_immediately(
        self, mock_genai: MagicMock
    ) -> None:
        """ValueError is not retried — raises immediately."""
        mock_model = MagicMock()
        mock_model.generate_content_async = AsyncMock(side_effect=ValueError("unexpected error"))
        mock_genai.GenerativeModel.return_value = mock_model

        client = GeminiClient(api_key="fake_key")

        with pytest.raises(ValueError):
            await client.parse("job text")

        assert mock_model.generate_content_async.call_count == 1
