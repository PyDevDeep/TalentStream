"""
Unit tests for app/clients/serper.py.

Coverage targets:
- SerperClient.search(): success, 429 retry, 5xx max retries
- SerperClient.view(): success, timeout retry
- _is_retryable(): pure logic
"""

from collections.abc import Mapping
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.clients.serper import SerperClient, _is_retryable

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def api_key() -> str:
    """Dummy API key for SerperClient instantiation."""
    return "test-api-key-123"


@pytest.fixture
def client(api_key: str) -> SerperClient:
    """SerperClient instance with test key."""
    return SerperClient(api_key=api_key, timeout=5.0)


def _make_response(status_code: int, json_data: Mapping[str, object] | None = None) -> MagicMock:
    """Build a fake httpx.Response mock."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = dict(json_data) if json_data else {}

    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            message=f"HTTP {status_code}",
            request=MagicMock(),
            response=resp,
        )
    else:
        resp.raise_for_status.return_value = None

    return resp


# ---------------------------------------------------------------------------
# _is_retryable
# ---------------------------------------------------------------------------


class TestIsRetryable:
    """Unit tests for the _is_retryable helper."""

    def test_timeout_exception_is_retryable(self) -> None:
        exc = httpx.TimeoutException("timed out")
        assert _is_retryable(exc) is True

    def test_429_is_retryable(self) -> None:
        resp = _make_response(429)
        exc = httpx.HTTPStatusError("rate limit", request=MagicMock(), response=resp)
        assert _is_retryable(exc) is True

    def test_500_is_retryable(self) -> None:
        resp = _make_response(500)
        exc = httpx.HTTPStatusError("server error", request=MagicMock(), response=resp)
        assert _is_retryable(exc) is True

    def test_503_is_retryable(self) -> None:
        resp = _make_response(503)
        exc = httpx.HTTPStatusError("unavailable", request=MagicMock(), response=resp)
        assert _is_retryable(exc) is True

    def test_404_is_not_retryable(self) -> None:
        resp = _make_response(404)
        exc = httpx.HTTPStatusError("not found", request=MagicMock(), response=resp)
        assert _is_retryable(exc) is False

    def test_400_is_not_retryable(self) -> None:
        resp = _make_response(400)
        exc = httpx.HTTPStatusError("bad request", request=MagicMock(), response=resp)
        assert _is_retryable(exc) is False

    def test_generic_exception_is_not_retryable(self) -> None:
        assert _is_retryable(ValueError("something")) is False


# ---------------------------------------------------------------------------
# SerperClient.search
# ---------------------------------------------------------------------------


class TestSerperClientSearch:
    """Tests for SerperClient.search()."""

    @pytest.mark.asyncio
    async def test_search_success_returns_url_list(self, client: SerperClient) -> None:
        """200 response with organic results → list of URLs."""
        json_data = {
            "organic": [
                {"link": "https://example.com/job/1"},
                {"link": "https://example.com/job/2"},
                {"title": "No link item"},
            ]
        }
        mock_resp = _make_response(200, json_data)

        with patch.object(client.client, "post", new=AsyncMock(return_value=mock_resp)):
            result = await client.search("Python Developer", num_results=3)

        assert result == ["https://example.com/job/1", "https://example.com/job/2"]

    @pytest.mark.asyncio
    async def test_search_empty_organic_returns_empty_list(self, client: SerperClient) -> None:
        """Response with no organic key → empty list."""
        mock_resp = _make_response(200, {})

        with patch.object(client.client, "post", new=AsyncMock(return_value=mock_resp)):
            result = await client.search("Python Developer")

        assert result == []

    @pytest.mark.asyncio
    async def test_search_retry_on_429_then_success(self, client: SerperClient) -> None:
        """Two 429 responses followed by 200 → returns result after retries."""
        json_data = {"organic": [{"link": "https://example.com/job/1"}]}
        resp_429 = _make_response(429)
        resp_200 = _make_response(200, json_data)

        call_count = 0

        async def side_effect(*args, **kwargs):  # type: ignore[no-untyped-def]
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return resp_429
            return resp_200

        with patch.object(client.client, "post", new=AsyncMock(side_effect=side_effect)):
            result = await client.search("Python Developer")

        assert result == ["https://example.com/job/1"]
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_search_max_retries_raises_http_status_error(self, client: SerperClient) -> None:
        """5 consecutive 500 responses → raises HTTPStatusError after max retries."""
        resp_500 = _make_response(500)

        with patch.object(client.client, "post", new=AsyncMock(return_value=resp_500)):
            with pytest.raises(httpx.HTTPStatusError):
                await client.search("Python Developer")

    @pytest.mark.asyncio
    async def test_search_passes_correct_payload(self, client: SerperClient) -> None:
        """Verifies correct JSON body is sent to Serper API."""
        mock_resp = _make_response(200, {"organic": []})
        mock_post = AsyncMock(return_value=mock_resp)

        with patch.object(client.client, "post", new=mock_post):
            await client.search("Senior Python", num_results=5)

        mock_post.assert_called_once_with(
            "https://google.serper.dev/search",
            json={"q": "Senior Python", "num": 5},
        )


# ---------------------------------------------------------------------------
# SerperClient.view
# ---------------------------------------------------------------------------


class TestSerperClientView:
    """Tests for SerperClient.view()."""

    @pytest.mark.asyncio
    async def test_view_success_returns_text(self, client: SerperClient) -> None:
        """200 response with text field → returns text content."""
        mock_resp = _make_response(200, {"text": "Job description here"})

        with patch.object(client.client, "post", new=AsyncMock(return_value=mock_resp)):
            result = await client.view("https://example.com/job/1")

        assert result == "Job description here"

    @pytest.mark.asyncio
    async def test_view_empty_text_returns_empty_string(self, client: SerperClient) -> None:
        """Response with no text key → returns empty string."""
        mock_resp = _make_response(200, {})

        with patch.object(client.client, "post", new=AsyncMock(return_value=mock_resp)):
            result = await client.view("https://example.com/job/1")

        assert result == ""

    @pytest.mark.asyncio
    async def test_view_timeout_triggers_retry(self, client: SerperClient) -> None:
        """TimeoutException on first call → retries and succeeds on second."""
        mock_resp = _make_response(200, {"text": "job content"})
        call_count = 0

        async def side_effect(*args, **kwargs):  # type: ignore[no-untyped-def]
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.TimeoutException("timeout")
            return mock_resp

        with patch.object(client.client, "post", new=AsyncMock(side_effect=side_effect)):
            result = await client.view("https://example.com/job/1")

        assert result == "job content"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_view_passes_correct_url_payload(self, client: SerperClient) -> None:
        """Verifies correct JSON body is sent to Serper Scrape API."""
        mock_resp = _make_response(200, {"text": "content"})
        mock_post = AsyncMock(return_value=mock_resp)

        with patch.object(client.client, "post", new=mock_post):
            await client.view("https://example.com/job/42")

        mock_post.assert_called_once_with(
            "https://scrape.serper.dev",
            json={"url": "https://example.com/job/42"},
        )

    @pytest.mark.asyncio
    async def test_view_max_retries_raises_http_status_error(self, client: SerperClient) -> None:
        """5 consecutive 500 responses on view → raises HTTPStatusError."""
        resp_500 = _make_response(500)

        with patch.object(client.client, "post", new=AsyncMock(return_value=resp_500)):
            with pytest.raises(httpx.HTTPStatusError):
                await client.view("https://example.com/job/1")


# ---------------------------------------------------------------------------
# SerperClient.close
# ---------------------------------------------------------------------------


class TestSerperClientClose:
    """Tests for graceful shutdown."""

    @pytest.mark.asyncio
    async def test_close_calls_aclose(self, client: SerperClient) -> None:
        """close() delegates to httpx client aclose."""
        with patch.object(client.client, "aclose", new=AsyncMock()) as mock_aclose:
            await client.close()
        mock_aclose.assert_called_once()
