import asyncio

import structlog

from app.broker import broker
from app.config import get_settings
from app.db.repository import JobRepository
from app.db.session import get_session
from app.notifications.slack import SlackNotifier

logger = structlog.get_logger()


@broker.task(task_name="send_alert")
async def send_alert() -> dict[str, int]:
    """Fetch unnotified jobs and publish each one to Slack."""
    log = logger.bind(task="send_alert")
    log.info("send_alert_started")

    settings = get_settings()

    sent_count = 0
    failed_count = 0

    async with get_session() as session:
        repo = JobRepository(session)
        unnotified_jobs = await repo.get_unnotified(limit=50)

        if not unnotified_jobs:
            log.info("no_unnotified_jobs_found")
            return {"sent": 0, "failed": 0}

        notifier = SlackNotifier(
            bot_token=settings.slack_bot_token.get_secret_value(),
            channel_id=settings.slack_channel_id,
        )

        try:
            for job in unnotified_jobs:
                success = await notifier.send(job)
                if success:
                    await repo.mark_notified(job.id)
                    sent_count += 1
                else:
                    failed_count += 1

                # Slack API hard rate limit: 1 message per second.
                await asyncio.sleep(1)
        finally:
            await notifier.close()

    log.info("send_alert_completed", sent=sent_count, failed=failed_count)
    return {"sent": sent_count, "failed": failed_count}
