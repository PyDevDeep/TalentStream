"""
Unit tests for app/clients/llm/openai_client.py.

Coverage targets:
- parse_with_openai(): success, RateLimitError retry, APIError retry, unknown exception
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from openai import APIError, RateLimitError

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


def _make_api_error() -> APIError:
    """APIError requires a request object."""
    request = MagicMock()
    return APIError(
        message="internal server error",
        request=request,
        body=None,
    )


# ---------------------------------------------------------------------------
# parse_with_openai
# ---------------------------------------------------------------------------


class TestParseWithOpenai:
    """Tests for parse_with_openai() function."""

    @pytest.mark.asyncio
    async def test_parse_success_returns_json_string(self) -> None:
        """Valid API response → returns message content as string."""
        expected = '{"title": "Python Dev", "company": "Acme"}'
        completion = _make_completion(expected)

        with patch("app.clients.llm.openai_client._get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_client.chat.completions.create = AsyncMock(return_value=completion)
            from app.clients.llm.openai_client import parse_with_openai

            result = await parse_with_openai("Some job text")

        assert result == expected

    @pytest.mark.asyncio
    async def test_parse_sends_correct_model_and_format(self) -> None:
        """Verifies gpt-4o-mini, json_object format, and temperature=0.0 are used."""
        completion = _make_completion("{}")

        with patch("app.clients.llm.openai_client._get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_post = AsyncMock(return_value=completion)
            mock_client.chat.completions.create = mock_post
            from app.clients.llm.openai_client import parse_with_openai

            await parse_with_openai("job text")

        call_kwargs = mock_post.call_args.kwargs
        assert call_kwargs["model"] == "gpt-4o-mini"
        assert call_kwargs["response_format"] == {"type": "json_object"}
        assert call_kwargs["temperature"] == 0.0

    @pytest.mark.asyncio
    async def test_parse_injects_job_text_into_user_message(self) -> None:
        """Job text is placed in the user message content."""
        completion = _make_completion("{}")

        with patch("app.clients.llm.openai_client._get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_post = AsyncMock(return_value=completion)
            mock_client.chat.completions.create = mock_post
            from app.clients.llm.openai_client import parse_with_openai

            await parse_with_openai("unique job text here")

        messages = mock_post.call_args.kwargs["messages"]
        user_msg = next(m for m in messages if m["role"] == "user")
        assert "unique job text here" in user_msg["content"]

    @pytest.mark.asyncio
    async def test_parse_retries_on_rate_limit_error(self) -> None:
        """RateLimitError on first call → retries and succeeds on second."""
        completion = _make_completion('{"title": "Dev"}')
        call_count = 0

        async def side_effect(**kwargs):  # type: ignore[no-untyped-def]
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise _make_rate_limit_error()
            return completion

        with patch("app.clients.llm.openai_client._get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_client.chat.completions.create = AsyncMock(side_effect=side_effect)
            from app.clients.llm.openai_client import parse_with_openai

            result = await parse_with_openai("job text")

        assert result == '{"title": "Dev"}'
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_parse_retries_on_api_error(self) -> None:
        """APIError on first call → retries and succeeds on second."""
        completion = _make_completion('{"title": "Dev"}')
        call_count = 0

        async def side_effect(**kwargs):  # type: ignore[no-untyped-def]
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise _make_api_error()
            return completion

        with patch("app.clients.llm.openai_client._get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_client.chat.completions.create = AsyncMock(side_effect=side_effect)
            from app.clients.llm.openai_client import parse_with_openai

            result = await parse_with_openai("job text")

        assert result == '{"title": "Dev"}'
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_parse_max_retries_raises_rate_limit_error(self) -> None:
        """3 consecutive RateLimitErrors → raises after max retries."""
        with patch("app.clients.llm.openai_client._get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_client.chat.completions.create = AsyncMock(side_effect=_make_rate_limit_error())
            from app.clients.llm.openai_client import parse_with_openai

            with pytest.raises(RateLimitError):
                await parse_with_openai("job text")

    @pytest.mark.asyncio
    async def test_parse_unexpected_exception_raises_immediately(self) -> None:
        """Non-retryable exception (e.g. ValueError) → raises immediately without retry."""
        call_count = 0

        async def side_effect(**kwargs):  # type: ignore[no-untyped-def]
            nonlocal call_count
            call_count += 1
            raise ValueError("unexpected")

        with patch("app.clients.llm.openai_client._get_client") as mock_get_client:
            mock_client = MagicMock()
            mock_get_client.return_value = mock_client
            mock_client.chat.completions.create = AsyncMock(side_effect=side_effect)
            from app.clients.llm.openai_client import parse_with_openai

            with pytest.raises(ValueError):
                await parse_with_openai("job text")

        # tenacity only retries RateLimitError/APIError — ValueError should not retry
        assert call_count == 1
