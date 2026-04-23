"""
Unit tests for app/notifications/slack.py.

Coverage targets:
- SlackNotifier.send(): success, SlackApiError, unexpected exception
- SlackNotifier._format_block_kit(): salary variants, title truncation,
  skills block, no location
- SlackNotifier.close(): no-op
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from slack_sdk.errors import SlackApiError

from app.notifications.slack import SlackNotifier

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_job(
    *,
    title: str = "Python Developer",
    company: str = "Acme Corp",
    location: str | None = "Kyiv, Ukraine",
    salary_min: int | None = 4000,
    salary_max: int | None = 6000,
    salary_currency: str = "USD",
    skills: list[str] | None = None,
    source_url: str = "https://example.com/job/1",
    job_id: int = 42,
) -> MagicMock:
    """Build a minimal Job ORM mock."""
    job = MagicMock()
    job.id = job_id
    job.title = title
    job.company = company
    job.location = location
    job.salary_min = salary_min
    job.salary_max = salary_max
    job.salary_currency = salary_currency
    job.skills = skills if skills is not None else ["Python", "FastAPI"]
    job.source_url = source_url
    return job


@pytest.fixture
def notifier() -> SlackNotifier:
    """SlackNotifier with test credentials."""
    return SlackNotifier(bot_token="xoxb-test", channel_id="C0TEST123")


# ---------------------------------------------------------------------------
# send()
# ---------------------------------------------------------------------------


class TestSlackNotifierSend:
    """Tests for SlackNotifier.send()."""

    @pytest.mark.asyncio
    async def test_send_success_returns_true(self, notifier: SlackNotifier) -> None:
        """Successful chat_postMessage → returns True."""
        mock_response = {"ts": "1234567890.000"}
        notifier.client.chat_postMessage = AsyncMock(return_value=mock_response)

        result = await notifier.send(_make_job())

        assert result is True

    @pytest.mark.asyncio
    async def test_send_calls_correct_channel(self, notifier: SlackNotifier) -> None:
        """Message is sent to the configured channel_id."""
        notifier.client.chat_postMessage = AsyncMock(return_value={"ts": "1234"})

        await notifier.send(_make_job())

        call_kwargs = notifier.client.chat_postMessage.call_args.kwargs
        assert call_kwargs["channel"] == "C0TEST123"

    @pytest.mark.asyncio
    async def test_send_disables_unfurl(self, notifier: SlackNotifier) -> None:
        """unfurl_links=False is passed to prevent Slack link previews."""
        notifier.client.chat_postMessage = AsyncMock(return_value={"ts": "1234"})

        await notifier.send(_make_job())

        call_kwargs = notifier.client.chat_postMessage.call_args.kwargs
        assert call_kwargs["unfurl_links"] is False

    @pytest.mark.asyncio
    async def test_send_slack_api_error_returns_false(self, notifier: SlackNotifier) -> None:
        """SlackApiError → returns False without raising."""
        error_response = MagicMock()
        error_response.__getitem__ = MagicMock(return_value="invalid_auth")
        notifier.client.chat_postMessage = AsyncMock(
            side_effect=SlackApiError("invalid_auth", error_response)
        )

        result = await notifier.send(_make_job())

        assert result is False

    @pytest.mark.asyncio
    async def test_send_unexpected_exception_returns_false(self, notifier: SlackNotifier) -> None:
        """Unexpected exception (network, etc.) → returns False without raising."""
        notifier.client.chat_postMessage = AsyncMock(side_effect=Exception("network error"))

        result = await notifier.send(_make_job())

        assert result is False


# ---------------------------------------------------------------------------
# _format_block_kit()
# ---------------------------------------------------------------------------


class TestSlackNotifierFormatBlockKit:
    """Tests for SlackNotifier._format_block_kit()."""

    def test_header_block_contains_job_title(self, notifier: SlackNotifier) -> None:
        """First block is header with job title."""
        blocks = notifier._format_block_kit(_make_job(title="Senior Dev"))
        header = blocks[0]
        assert header["type"] == "header"
        assert header["text"]["text"] == "Senior Dev"

    def test_long_title_truncated_to_148_chars(self, notifier: SlackNotifier) -> None:
        """Title longer than 150 chars → truncated with '...' suffix."""
        long_title = "x" * 160
        blocks = notifier._format_block_kit(_make_job(title=long_title))
        header_text = blocks[0]["text"]["text"]
        assert len(header_text) <= 150
        assert header_text.endswith("...")

    def test_salary_range_formatted_correctly(self, notifier: SlackNotifier) -> None:
        """salary_min + salary_max → 'min - max CURRENCY' format."""
        blocks = notifier._format_block_kit(
            _make_job(salary_min=4000, salary_max=6000, salary_currency="USD")
        )
        section_text = blocks[1]["text"]["text"]
        assert "4000 - 6000 USD" in section_text

    def test_salary_min_only_formatted_as_from(self, notifier: SlackNotifier) -> None:
        """Only salary_min → 'From X CURRENCY' format."""
        blocks = notifier._format_block_kit(_make_job(salary_min=3000, salary_max=None))
        section_text = blocks[1]["text"]["text"]
        assert "From 3000 USD" in section_text

    def test_no_salary_shows_na(self, notifier: SlackNotifier) -> None:
        """No salary fields → shows 'N/A'."""
        blocks = notifier._format_block_kit(_make_job(salary_min=None, salary_max=None))
        section_text = blocks[1]["text"]["text"]
        assert "N/A" in section_text

    def test_no_location_shows_remote(self, notifier: SlackNotifier) -> None:
        """job.location=None → shows 'Remote / N/A'."""
        blocks = notifier._format_block_kit(_make_job(location=None))
        section_text = blocks[1]["text"]["text"]
        assert "Remote / N/A" in section_text

    def test_skills_block_added_when_skills_present(self, notifier: SlackNotifier) -> None:
        """Job with skills → skills block present in output."""
        blocks = notifier._format_block_kit(_make_job(skills=["Python", "Redis"]))
        skills_block = next(
            (
                b
                for b in blocks
                if b.get("type") == "section" and "*Skills:*" in b.get("text", {}).get("text", "")
            ),
            None,
        )
        assert skills_block is not None
        assert "Python" in skills_block["text"]["text"]
        assert "Redis" in skills_block["text"]["text"]

    def test_no_skills_block_when_empty(self, notifier: SlackNotifier) -> None:
        """Job with empty skills list → no skills section block."""
        blocks = notifier._format_block_kit(_make_job(skills=[]))
        skills_block = next(
            (
                b
                for b in blocks
                if b.get("type") == "section" and "*Skills:*" in b.get("text", {}).get("text", "")
            ),
            None,
        )
        assert skills_block is None

    def test_actions_block_contains_source_url(self, notifier: SlackNotifier) -> None:
        """Last block is 'actions' with 'View Job' button pointing to source_url."""
        url = "https://example.com/job/99"
        blocks = notifier._format_block_kit(_make_job(source_url=url))
        actions = blocks[-1]
        assert actions["type"] == "actions"
        button = actions["elements"][0]
        assert button["url"] == url
        assert button["text"]["text"] == "View Job"


# ---------------------------------------------------------------------------
# close()
# ---------------------------------------------------------------------------


class TestSlackNotifierClose:
    """Tests for SlackNotifier.close()."""

    @pytest.mark.asyncio
    async def test_close_is_noop(self, notifier: SlackNotifier) -> None:
        """close() completes without raising."""
        await notifier.close()
