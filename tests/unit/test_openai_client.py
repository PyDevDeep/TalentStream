"""
Unit tests for app/clients/llm/openai_client.py.

Coverage targets:
- OpenAIClient.parse(): success, text truncation, RateLimitError retry, max retries, unknown exception
"""

from typing import Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from openai import RateLimitError

from app.clients.llm.openai_client import OpenAIClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_completion(content: str) -> MagicMock:
    """Build a minimal ChatCompletion mock."""
    msg = MagicMock()
    msg.content = content
    choice = MagicMock()
    choice.message = msg
    completion = MagicMock()
    completion.choices = [choice]
    return completion


def _make_rate_limit_error() -> RateLimitError:
    """RateLimitError requires a response object."""
    response = MagicMock()
    response.status_code = 429
    response.headers = {}
    response.json.return_value = {"error": {"message": "rate limit"}}
    return RateLimitError(
        message="rate limit exceeded",
        response=response,
        body={"error": {"message": "rate limit"}},
    )


# ---------------------------------------------------------------------------
# TestOpenAIClient
# ---------------------------------------------------------------------------


class TestOpenAIClient:
    """Tests for OpenAIClient class."""

    @pytest.fixture
    def mock_openai(self) -> Generator[MagicMock, None, None]:
        # Мокаємо сам клас AsyncOpenAI з офіційної бібліотеки
        with patch("app.clients.llm.openai_client.AsyncOpenAI") as mock:
            yield mock

    @pytest.mark.asyncio
    async def test_parse_success(self, mock_openai: MagicMock) -> None:
        """Valid response returns content."""
        mock_client_instance = mock_openai.return_value
        mock_client_instance.chat.completions.create = AsyncMock(
            return_value=_make_completion('{"title": "Dev"}')
        )

        client = OpenAIClient(api_key="fake_key")
        result = await client.parse("job text")

        assert result == '{"title": "Dev"}'
        mock_client_instance.chat.completions.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_parse_truncates_long_text(self, mock_openai: MagicMock) -> None:
        """Text > 20000 chars is truncated before sending to model."""
        mock_client_instance = mock_openai.return_value
        mock_client_instance.chat.completions.create = AsyncMock(
            return_value=_make_completion("{}")
        )

        client = OpenAIClient(api_key="fake_key")
        # Створюємо рядок на 30 000 символів
        long_text = "x" * 30_000
        await client.parse(long_text)

        call_args = mock_client_instance.chat.completions.create.call_args[1]
        messages = call_args["messages"]
        user_prompt = messages[1]["content"]  # 0 = system, 1 = user

        assert "x" * 20_000 in user_prompt
        assert "x" * 20_001 not in user_prompt

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_parse_retries_on_rate_limit(
        self, mock_sleep: AsyncMock, mock_openai: MagicMock
    ) -> None:
        """RateLimitError on first call → retries and succeeds on second."""
        mock_client_instance = mock_openai.return_value
        mock_client_instance.chat.completions.create = AsyncMock(
            side_effect=[_make_rate_limit_error(), _make_completion('{"title": "Dev"}')]
        )

        client = OpenAIClient(api_key="fake_key")
        result = await client.parse("job text")

        assert result == '{"title": "Dev"}'
        assert mock_client_instance.chat.completions.create.call_count == 2

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_parse_max_retries_raises(
        self, mock_sleep: AsyncMock, mock_openai: MagicMock
    ) -> None:
        """3 consecutive RateLimitErrors → raises after max retries."""
        mock_client_instance = mock_openai.return_value
        mock_client_instance.chat.completions.create = AsyncMock(
            side_effect=_make_rate_limit_error()
        )

        client = OpenAIClient(api_key="fake_key")

        with pytest.raises(RateLimitError):
            await client.parse("job text")

        assert mock_client_instance.chat.completions.create.call_count == 3

    @pytest.mark.asyncio
    async def test_parse_non_retryable_exception_raises_immediately(
        self, mock_openai: MagicMock
    ) -> None:
        """Non-retryable exception (e.g. ValueError) → raises immediately without retry."""
        mock_client_instance = mock_openai.return_value
        mock_client_instance.chat.completions.create = AsyncMock(
            side_effect=ValueError("unexpected")
        )

        client = OpenAIClient(api_key="fake_key")

        with pytest.raises(ValueError):
            await client.parse("job text")

        assert mock_client_instance.chat.completions.create.call_count == 1
