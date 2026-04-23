"""
Unit tests for app/broker.py.

Coverage targets:
- on_task_error(): logs error, captures to Sentry when dsn set, skips Sentry when no dsn
"""

from unittest.mock import MagicMock, patch

from taskiq import TaskiqMessage

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_message(task_name: str = "test_task") -> TaskiqMessage:
    """Build a minimal TaskiqMessage mock."""
    msg = MagicMock(spec=TaskiqMessage)
    msg.task_name = task_name
    return msg


# ---------------------------------------------------------------------------
# on_task_error
# ---------------------------------------------------------------------------


class TestOnTaskError:
    """Tests for on_task_error() handler."""

    def test_logs_error_always(self) -> None:
        """on_task_error always logs task_name and error string."""
        with patch("app.broker.settings") as mock_settings:
            mock_settings.sentry_dsn = None
            with patch("app.broker.logger") as mock_logger:
                from app.broker import on_task_error

                on_task_error(_make_message("my_task"), ValueError("boom"))

        mock_logger.error.assert_called_once()
        call_kwargs = mock_logger.error.call_args
        # structlog uses positional event + keyword args
        assert "my_task" in str(call_kwargs)
        assert "boom" in str(call_kwargs)

    def test_captures_to_sentry_when_dsn_set(self) -> None:
        """When sentry_dsn is set → sentry_sdk.capture_exception is called."""
        exc = RuntimeError("task failed")
        msg = _make_message("failing_task")

        with patch("app.broker.settings") as mock_settings:
            mock_settings.sentry_dsn = "https://fake@sentry.io/123"
            with patch("app.broker.sentry_sdk") as mock_sentry:
                mock_scope = MagicMock()
                mock_sentry.push_scope.return_value.__enter__ = MagicMock(return_value=mock_scope)
                mock_sentry.push_scope.return_value.__exit__ = MagicMock(return_value=False)

                from app.broker import on_task_error

                on_task_error(msg, exc)

        mock_sentry.capture_exception.assert_called_once_with(exc)

    def test_sets_task_name_tag_in_sentry_scope(self) -> None:
        """Sentry scope gets task_name tag set."""
        exc = RuntimeError("fail")
        msg = _make_message("tagged_task")

        with patch("app.broker.settings") as mock_settings:
            mock_settings.sentry_dsn = "https://fake@sentry.io/123"
            with patch("app.broker.sentry_sdk") as mock_sentry:
                mock_scope = MagicMock()
                mock_sentry.push_scope.return_value.__enter__ = MagicMock(return_value=mock_scope)
                mock_sentry.push_scope.return_value.__exit__ = MagicMock(return_value=False)

                from app.broker import on_task_error

                on_task_error(msg, exc)

        mock_scope.set_tag.assert_called_once_with("task_name", "tagged_task")

    def test_skips_sentry_when_no_dsn(self) -> None:
        """When sentry_dsn is None → sentry_sdk.capture_exception not called."""
        with patch("app.broker.settings") as mock_settings:
            mock_settings.sentry_dsn = None
            with patch("app.broker.sentry_sdk") as mock_sentry:
                from app.broker import on_task_error

                on_task_error(_make_message(), ValueError("no sentry"))

        mock_sentry.capture_exception.assert_not_called()
