"""
Unit tests for app/broker.py.

Coverage targets:
- ErrorLoggingMiddleware.on_error(): logs error, captures to Sentry when dsn set, skips Sentry when no dsn
"""

from unittest.mock import MagicMock, patch

from taskiq import TaskiqMessage, TaskiqResult

from app.broker import ErrorLoggingMiddleware

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_message(task_name: str = "test_task") -> TaskiqMessage:
    """Build a minimal TaskiqMessage mock."""
    msg = MagicMock(spec=TaskiqMessage)
    msg.task_name = task_name
    msg.task_id = "test-id-123"
    return msg


def _make_result() -> TaskiqResult[bytes]:
    """Build a minimal TaskiqResult mock."""
    return MagicMock(spec=TaskiqResult)


# ---------------------------------------------------------------------------
# ErrorLoggingMiddleware.on_error
# ---------------------------------------------------------------------------


class TestErrorLoggingMiddleware:
    """Tests for ErrorLoggingMiddleware.on_error()."""

    def test_logs_error_always(self) -> None:
        """on_error always logs task_name and error string."""
        middleware = ErrorLoggingMiddleware()

        with patch("app.broker.get_settings") as mock_get_settings:
            mock_get_settings.return_value.sentry_dsn = None
            with patch("app.broker.logger") as mock_logger:
                middleware.on_error(_make_message("my_task"), _make_result(), ValueError("boom"))

        mock_logger.error.assert_called_once()
        call_kwargs = mock_logger.error.call_args
        assert "my_task" in str(call_kwargs)
        assert "boom" in str(call_kwargs)

    def test_captures_to_sentry_when_dsn_set(self) -> None:
        """When sentry_dsn is set → sentry_sdk.capture_exception is called."""
        middleware = ErrorLoggingMiddleware()
        exc = RuntimeError("task failed")
        msg = _make_message("failing_task")

        with patch("app.broker.get_settings") as mock_get_settings:
            mock_get_settings.return_value.sentry_dsn = "https://fake@sentry.io/123"
            with patch("app.broker.sentry_sdk") as mock_sentry:
                mock_scope = MagicMock()
                mock_sentry.push_scope.return_value.__enter__ = MagicMock(return_value=mock_scope)
                mock_sentry.push_scope.return_value.__exit__ = MagicMock(return_value=False)

                middleware.on_error(msg, _make_result(), exc)

        mock_sentry.capture_exception.assert_called_once_with(exc)

    def test_sets_task_name_tag_in_sentry_scope(self) -> None:
        """Sentry scope gets task_name tag set."""
        middleware = ErrorLoggingMiddleware()
        exc = RuntimeError("fail")
        msg = _make_message("tagged_task")

        with patch("app.broker.get_settings") as mock_get_settings:
            mock_get_settings.return_value.sentry_dsn = "https://fake@sentry.io/123"
            with patch("app.broker.sentry_sdk") as mock_sentry:
                mock_scope = MagicMock()
                mock_sentry.push_scope.return_value.__enter__ = MagicMock(return_value=mock_scope)
                mock_sentry.push_scope.return_value.__exit__ = MagicMock(return_value=False)

                middleware.on_error(msg, _make_result(), exc)

        mock_scope.set_tag.assert_any_call("task_name", "tagged_task")

    def test_skips_sentry_when_no_dsn(self) -> None:
        """When sentry_dsn is None → sentry_sdk.capture_exception not called."""
        middleware = ErrorLoggingMiddleware()

        with patch("app.broker.get_settings") as mock_get_settings:
            mock_get_settings.return_value.sentry_dsn = None
            with patch("app.broker.sentry_sdk") as mock_sentry:
                middleware.on_error(_make_message(), _make_result(), ValueError("no sentry"))

        mock_sentry.capture_exception.assert_not_called()
