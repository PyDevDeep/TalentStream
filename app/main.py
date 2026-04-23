from contextlib import asynccontextmanager
from typing import AsyncGenerator

import sentry_sdk
import structlog
from fastapi import FastAPI
from pydantic import BaseModel

from app.config import get_settings
from app.scheduler import SCHEDULED_TASKS, redis_source
from app.tasks.scrape import scrape_job_page

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    settings = get_settings()
    if settings.sentry_dsn:
        sentry_sdk.init(
            dsn=str(settings.sentry_dsn),
            environment=settings.environment,
            traces_sample_rate=1.0,
        )
        logger.info("sentry_initialized")

    await redis_source.startup()
    for task in SCHEDULED_TASKS:
        await redis_source.add_schedule(task)
    logger.info("Schedules registered in Redis", task_count=len(SCHEDULED_TASKS))

    yield

    await redis_source.shutdown()
    logger.info("Redis schedule source disconnected")


app = FastAPI(title="TalentStream", lifespan=lifespan)


class ScrapeRequest(BaseModel):
    query: str


@app.post("/scrape")
async def trigger_scrape(request: ScrapeRequest) -> dict[str, str]:
    """Ручний запуск процесу пошуку вакансій."""
    task = await scrape_job_page.kiq(request.query)
    return {"task_id": task.task_id, "status": "queued", "query": request.query}
