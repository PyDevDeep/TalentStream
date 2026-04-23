"""
Unit tests for app/tasks/notify.py — send_alert task.

Coverage targets:
- send_alert(): no unnotified jobs, sends all, handles slack failure,
  marks notified only on success, closes notifier in finally
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_job(job_id: int = 1) -> MagicMock:
    job = MagicMock()
    job.id = job_id
    job.title = f"Job {job_id}"
    job.company = "Acme"
    return job


def _patch_send_alert(
    unnotified_jobs: list[MagicMock],
    send_results: list[bool],
) -> tuple[MagicMock, MagicMock]:
    """
    Returns (mock_repo, mock_notifier) with pre-configured return values.
    Caller is responsible for patching get_session and SlackNotifier.
    """
    mock_repo = MagicMock()
    mock_repo.get_unnotified = AsyncMock(return_value=unnotified_jobs)
    mock_repo.mark_notified = AsyncMock()

    mock_notifier = MagicMock()
    mock_notifier.send = AsyncMock(side_effect=send_results)
    mock_notifier.close = AsyncMock()

    return mock_repo, mock_notifier


# ---------------------------------------------------------------------------
# send_alert tests
# ---------------------------------------------------------------------------


class TestSendAlert:
    """Tests for send_alert() task function."""

    @pytest.mark.asyncio
    async def test_no_unnotified_jobs_returns_zeros(self) -> None:
        """No pending jobs → returns {sent: 0, failed: 0} without calling Slack."""
        mock_repo = MagicMock()
        mock_repo.get_unnotified = AsyncMock(return_value=[])

        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("app.tasks.notify.get_session", return_value=mock_session):
            with patch("app.tasks.notify.JobRepository", return_value=mock_repo):
                with patch("app.tasks.notify.SlackNotifier") as mock_notifier_cls:
                    from app.tasks.notify import send_alert

                    result = await send_alert()

        assert result == {"sent": 0, "failed": 0}
        mock_notifier_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_sends_all_jobs_and_marks_notified(self) -> None:
        """Two jobs, both succeed → sent=2, failed=0, both marked notified."""
        jobs = [_make_job(1), _make_job(2)]
        mock_repo, mock_notifier = _patch_send_alert(jobs, [True, True])

        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("app.tasks.notify.get_session", return_value=mock_session):
            with patch("app.tasks.notify.JobRepository", return_value=mock_repo):
                with patch("app.tasks.notify.SlackNotifier", return_value=mock_notifier):
                    with patch("app.tasks.notify.asyncio.sleep", new=AsyncMock()):
                        from app.tasks.notify import send_alert

                        result = await send_alert()

        assert result == {"sent": 2, "failed": 0}
        assert mock_repo.mark_notified.call_count == 2
        mock_repo.mark_notified.assert_any_call(1)
        mock_repo.mark_notified.assert_any_call(2)

    @pytest.mark.asyncio
    async def test_slack_failure_increments_failed_not_marked(self) -> None:
        """Slack send returns False → failed count incremented, not marked notified."""
        jobs = [_make_job(1)]
        mock_repo, mock_notifier = _patch_send_alert(jobs, [False])

        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("app.tasks.notify.get_session", return_value=mock_session):
            with patch("app.tasks.notify.JobRepository", return_value=mock_repo):
                with patch("app.tasks.notify.SlackNotifier", return_value=mock_notifier):
                    with patch("app.tasks.notify.asyncio.sleep", new=AsyncMock()):
                        from app.tasks.notify import send_alert

                        result = await send_alert()

        assert result == {"sent": 0, "failed": 1}
        mock_repo.mark_notified.assert_not_called()

    @pytest.mark.asyncio
    async def test_mixed_results(self) -> None:
        """3 jobs: success, failure, success → sent=2, failed=1."""
        jobs = [_make_job(1), _make_job(2), _make_job(3)]
        mock_repo, mock_notifier = _patch_send_alert(jobs, [True, False, True])

        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("app.tasks.notify.get_session", return_value=mock_session):
            with patch("app.tasks.notify.JobRepository", return_value=mock_repo):
                with patch("app.tasks.notify.SlackNotifier", return_value=mock_notifier):
                    with patch("app.tasks.notify.asyncio.sleep", new=AsyncMock()):
                        from app.tasks.notify import send_alert

                        result = await send_alert()

        assert result == {"sent": 2, "failed": 1}
        assert mock_repo.mark_notified.call_count == 2

    @pytest.mark.asyncio
    async def test_notifier_close_called_in_finally(self) -> None:
        """notifier.close() is called even if send raises unexpectedly."""
        jobs = [_make_job(1)]
        mock_repo = MagicMock()
        mock_repo.get_unnotified = AsyncMock(return_value=jobs)
        mock_repo.mark_notified = AsyncMock()

        mock_notifier = MagicMock()
        mock_notifier.send = AsyncMock(side_effect=Exception("slack exploded"))
        mock_notifier.close = AsyncMock()

        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("app.tasks.notify.get_session", return_value=mock_session):
            with patch("app.tasks.notify.JobRepository", return_value=mock_repo):
                with patch("app.tasks.notify.SlackNotifier", return_value=mock_notifier):
                    with patch("app.tasks.notify.asyncio.sleep", new=AsyncMock()):
                        from app.tasks.notify import send_alert

                        with pytest.raises(Exception, match="slack exploded"):
                            await send_alert()

        mock_notifier.close.assert_called_once()
