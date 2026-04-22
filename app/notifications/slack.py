from typing import Any

import structlog
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient

from app.models.job import Job

BlockKitBlock = dict[str, Any]

logger = structlog.get_logger()


class SlackNotifier:
    def __init__(self, bot_token: str, channel_id: str):
        """Ініціалізація асинхронного клієнта Slack."""
        self.client = AsyncWebClient(token=bot_token)
        self.channel_id = channel_id

    async def send(self, job: Job) -> bool:
        """
        Відправляє повідомлення у Slack за допомогою Block Kit.
        Повертає True, якщо повідомлення успішно доставлено.
        """
        blocks = self._format_block_kit(job)
        fallback_text = f"New Job: {job.title} at {job.company}"

        try:
            response = await self.client.chat_postMessage(  # type: ignore[no-untyped-call]
                channel=self.channel_id,
                text=fallback_text,
                blocks=blocks,
                unfurl_links=False,  # Вимикаємо стандартне прев'ю посилань Slack
            )
            logger.debug("slack_message_sent", job_id=job.id, ts=response["ts"])
            return True
        except SlackApiError as e:
            logger.error("slack_api_error", error=e.response["error"], job_id=job.id)
            return False
        except Exception as e:
            logger.error("slack_unexpected_error", error=str(e), job_id=job.id)
            return False

    def _format_block_kit(self, job: Job) -> list[BlockKitBlock]:
        """Форматує вакансію у масив блоків Block Kit."""
        # Slack обмежує довжину заголовку до 150 символів
        title = job.title[:145] + "..." if len(job.title) > 150 else job.title

        salary_text = "N/A"
        if job.salary_min and job.salary_max:
            salary_text = f"{job.salary_min} - {job.salary_max} {job.salary_currency}"
        elif job.salary_min:
            salary_text = f"From {job.salary_min} {job.salary_currency}"

        location = job.location or "Remote / N/A"

        blocks: list[BlockKitBlock] = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": title,
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Company:* {job.company}\n*Location:* {location}\n*Salary:* {salary_text}",
                },
            },
        ]

        if job.skills:
            # Slack section block text limit is 3000 chars, but skills won't exceed this.
            skills_text = ", ".join(job.skills)
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Skills:* {skills_text}",
                    },
                }
            )

        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "View Job",
                            "emoji": True,
                        },
                        "url": job.source_url,
                        "action_id": "view_job_action",
                    }
                ],
            }
        )

        return blocks

    async def close(self) -> None:
        """Очищення ресурсів клієнта (для дотримання контракту)."""
        # slack-sdk AsyncWebClient автоматично керує сесією,
        # але ми залишаємо метод для сумісності з `finally` блоком у тасці.
        pass
