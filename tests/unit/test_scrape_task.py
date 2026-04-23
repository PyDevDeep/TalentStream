"""
Unit tests for app/tasks/scrape.py — scrape_job_page task.

Coverage targets:
- scrape_job_page(): queues correct number of tasks, empty results,
  closes client on error, propagates exception
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestScrapeJobPage:
    """Tests for scrape_job_page() task."""

    @pytest.mark.asyncio
    async def test_success_returns_correct_counts(self) -> None:
        """3 URLs found → urls_found=3, tasks_queued=3."""
        urls = [f"https://example.com/job/{i}" for i in range(3)]
        mock_serper = MagicMock()
        mock_serper.search = AsyncMock(return_value=urls)
        mock_serper.close = AsyncMock()

        with patch("app.tasks.scrape.SerperClient", return_value=mock_serper):
            with patch("app.tasks.scrape.parse_job") as mock_parse_job:
                mock_parse_job.kiq = AsyncMock()
                from app.tasks.scrape import scrape_job_page

                result = await scrape_job_page("Python Developer")

        assert result["urls_found"] == 3
        assert result["tasks_queued"] == 3

    @pytest.mark.asyncio
    async def test_kiq_called_for_each_url(self) -> None:
        """parse_job.kiq is called once per URL found."""
        urls = ["https://example.com/job/1", "https://example.com/job/2"]
        mock_serper = MagicMock()
        mock_serper.search = AsyncMock(return_value=urls)
        mock_serper.close = AsyncMock()

        with patch("app.tasks.scrape.SerperClient", return_value=mock_serper):
            with patch("app.tasks.scrape.parse_job") as mock_parse_job:
                mock_parse_job.kiq = AsyncMock()
                from app.tasks.scrape import scrape_job_page

                await scrape_job_page("Python Developer")

        assert mock_parse_job.kiq.call_count == 2
        mock_parse_job.kiq.assert_any_call("https://example.com/job/1")
        mock_parse_job.kiq.assert_any_call("https://example.com/job/2")

    @pytest.mark.asyncio
    async def test_empty_search_results_queues_no_tasks(self) -> None:
        """Serper returns empty list → 0 tasks queued."""
        mock_serper = MagicMock()
        mock_serper.search = AsyncMock(return_value=[])
        mock_serper.close = AsyncMock()

        with patch("app.tasks.scrape.SerperClient", return_value=mock_serper):
            with patch("app.tasks.scrape.parse_job") as mock_parse_job:
                mock_parse_job.kiq = AsyncMock()
                from app.tasks.scrape import scrape_job_page

                result = await scrape_job_page("Obscure Query")

        assert result["urls_found"] == 0
        assert result["tasks_queued"] == 0
        mock_parse_job.kiq.assert_not_called()

    @pytest.mark.asyncio
    async def test_serper_search_raises_propagates_exception(self) -> None:
        """search() raises → exception propagates to caller."""
        mock_serper = MagicMock()
        mock_serper.search = AsyncMock(side_effect=Exception("API down"))
        mock_serper.close = AsyncMock()

        with patch("app.tasks.scrape.SerperClient", return_value=mock_serper):
            from app.tasks.scrape import scrape_job_page

            with pytest.raises(Exception, match="API down"):
                await scrape_job_page("Python Developer")

    @pytest.mark.asyncio
    async def test_client_closed_on_exception(self) -> None:
        """client.close() is called even when search raises."""
        mock_serper = MagicMock()
        mock_serper.search = AsyncMock(side_effect=Exception("API down"))
        mock_serper.close = AsyncMock()

        with patch("app.tasks.scrape.SerperClient", return_value=mock_serper):
            from app.tasks.scrape import scrape_job_page

            with pytest.raises(Exception, match="API down"):
                await scrape_job_page("Python Developer")

        mock_serper.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_client_closed_on_success(self) -> None:
        """client.close() is called after successful execution."""
        mock_serper = MagicMock()
        mock_serper.search = AsyncMock(return_value=[])
        mock_serper.close = AsyncMock()

        with patch("app.tasks.scrape.SerperClient", return_value=mock_serper):
            with patch("app.tasks.scrape.parse_job") as mock_parse_job:
                mock_parse_job.kiq = AsyncMock()
                from app.tasks.scrape import scrape_job_page

                await scrape_job_page("Python Developer")

        mock_serper.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_uses_num_results_3(self) -> None:
        """search is called with num_results=3 to conserve API credits."""
        mock_serper = MagicMock()
        mock_serper.search = AsyncMock(return_value=[])
        mock_serper.close = AsyncMock()

        with patch("app.tasks.scrape.SerperClient", return_value=mock_serper):
            with patch("app.tasks.scrape.parse_job") as mock_parse_job:
                mock_parse_job.kiq = AsyncMock()
                from app.tasks.scrape import scrape_job_page

                await scrape_job_page("Python Developer")

        mock_serper.search.assert_called_once_with(query="Python Developer", num_results=3)
