from taskiq import ScheduledTask, TaskiqScheduler
from taskiq_redis import RedisScheduleSource

from app.broker import broker
from app.config import get_settings

_settings = get_settings()

redis_source = RedisScheduleSource(str(_settings.redis_url))

scheduler = TaskiqScheduler(
    broker=broker,
    sources=[redis_source],
)

SCHEDULED_TASKS: list[ScheduledTask] = [
    ScheduledTask(
        schedule_id="scrape_job_page_cron",
        task_name="scrape_job_page",
        labels={},
        args=[_settings.scrape_query],
        kwargs={},
        cron=f"*/{_settings.scrape_interval_minutes} * * * *",
    ),
    ScheduledTask(
        schedule_id="send_alert_cron",
        task_name="send_alert",
        labels={},
        args=[],
        kwargs={},
        cron="*/5 * * * *",
    ),
]
