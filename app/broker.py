import structlog
from taskiq import TaskiqEvents, TaskiqMessage, TaskiqState
from taskiq.middlewares import SimpleRetryMiddleware
from taskiq_redis import ListQueueBroker, RedisAsyncResultBackend

from app.config import get_settings

settings = get_settings()
logger = structlog.get_logger()

redis_url = str(settings.redis_url)

result_backend: RedisAsyncResultBackend[bytes] = RedisAsyncResultBackend(
    redis_url=redis_url,
    keep_results=True,
    result_ex_time=3600,
)

broker = ListQueueBroker(
    url=redis_url,
    result_backend=result_backend,  # type: ignore[arg-type]
)

broker.add_middleware(SimpleRetryMiddleware(default_retry_count=3))  # type: ignore[union-attr]


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def startup(state: TaskiqState) -> None:
    """Initialize resources on worker startup."""
    logger.info("TaskIQ worker starting up...")


@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def shutdown(state: TaskiqState) -> None:
    """Clean up resources on worker shutdown."""
    logger.info("TaskIQ worker shutting down...")


def on_task_error(
    message: TaskiqMessage, exception: Exception, *_args: object, **_kwargs: object
) -> None:
    """Global task error handler."""
    logger.error("Task failed", task_name=message.task_name, error=str(exception))
