from typing import Any

from app.broker import broker


@broker.task(task_name="parse_job")
async def parse_job(url: str) -> dict[str, Any]:
    """Заглушка для парсингу вакансії. Буде реалізована в Task 3.1.2."""
    return {}
