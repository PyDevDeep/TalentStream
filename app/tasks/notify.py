from typing import Any

from app.broker import broker


@broker.task(task_name="send_alert")
async def send_alert() -> dict[str, Any]:
    """Заглушка для відправки сповіщень. Буде реалізована в Task 3.1.3."""
    return {}
