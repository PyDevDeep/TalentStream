import sentry_sdk
import structlog
from taskiq import TaskiqEvents, TaskiqMessage, TaskiqState
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

broker = (
    ListQueueBroker(url=_redis_url)
    .with_result_backend(result_backend)
    .with_middlewares(SimpleRetryMiddleware(default_retry_count=3))
)


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def startup(state: TaskiqState) -> None:
    """Initialize resources on worker startup."""
    logger.info("TaskIQ worker starting up...")


@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def shutdown(state: TaskiqState) -> None:
    """Clean up resources on worker shutdown."""
    logger.info("TaskIQ worker shutting down...")


def on_task_error(
    message: TaskiqMessage, exception: Exception, *args: object, **_kwargs: object
) -> None:
    """Global task error handler — logs and optionally reports to Sentry."""
    logger.error("Task failed", task_name=message.task_name, error=str(exception))
    if get_settings().sentry_dsn:
        with sentry_sdk.push_scope() as scope:
            scope.set_tag("task_name", message.task_name)
            sentry_sdk.capture_exception(exception)
