from typing import Any

import sentry_sdk
import structlog
from redis.asyncio import Redis
from taskiq import TaskiqEvents, TaskiqMessage, TaskiqMiddleware, TaskiqResult, TaskiqState
from taskiq.middlewares import SimpleRetryMiddleware
from taskiq_redis import ListQueueBroker, RedisAsyncResultBackend

from app.config import get_settings

logger = structlog.get_logger()

_redis_url = str(get_settings().redis_url)

result_backend: RedisAsyncResultBackend[bytes] = RedisAsyncResultBackend(
    redis_url=_redis_url,
    keep_results=True,
    result_ex_time=3600,
)


class ErrorLoggingMiddleware(TaskiqMiddleware):
    """Middleware that logs task failures and reports them to Sentry."""

    def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
        exception: BaseException,
    ) -> None:
        logger.error(
            "task_failed",
            task_name=message.task_name,
            task_id=message.task_id,
            error=str(exception),
        )
        if get_settings().sentry_dsn:
            with sentry_sdk.push_scope() as scope:
                scope.set_tag("task_name", message.task_name)
                scope.set_tag("task_id", message.task_id)
                sentry_sdk.capture_exception(exception)


broker = (
    ListQueueBroker(url=_redis_url)
    .with_result_backend(result_backend)
    .with_middlewares(
        SimpleRetryMiddleware(default_retry_count=3),
        ErrorLoggingMiddleware(),
    )
)


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def startup(state: TaskiqState) -> None:
    """Initialize shared resources on worker startup."""
    settings = get_settings()
    state.redis_client = Redis.from_url(str(settings.redis_url))  # type: ignore[reportUnknownMemberType]
    logger.info("taskiq_worker_started", redis_url=str(settings.redis_url)[:20] + "...")


@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def shutdown(state: TaskiqState) -> None:
    """Clean up shared resources on worker shutdown."""
    if hasattr(state, "redis_client"):
        await state.redis_client.aclose()
    logger.info("taskiq_worker_stopped")
