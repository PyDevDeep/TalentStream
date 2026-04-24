from contextlib import asynccontextmanager
from typing import AsyncGenerator

import sentry_sdk
import structlog
from fastapi import APIRouter, FastAPI
from pydantic import BaseModel

from app.broker import broker
from app.config import get_settings
from app.scheduler import SCHEDULED_TASKS, redis_source
from app.tasks.scrape import scrape_job_page

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    settings = get_settings()

    # --- Sentry ---
    if settings.sentry_dsn:
        sentry_sdk.init(
            dsn=str(settings.sentry_dsn),
            environment=settings.environment,
            traces_sample_rate=1.0,
        )
        logger.info("sentry_initialized")

    # --- Broker ---
    await broker.startup()
    logger.info("broker_started")

    # --- Scheduler source ---
    await redis_source.startup()
    for task in SCHEDULED_TASKS:
        await redis_source.add_schedule(task)
    logger.info("schedules_registered", task_count=len(SCHEDULED_TASKS))

    yield

    # --- Shutdown ---
    await redis_source.shutdown()
    logger.info("redis_schedule_source_disconnected")

    await broker.shutdown()
    logger.info("broker_stopped")


router = APIRouter()


class ScrapeRequest(BaseModel):
    query: str


@router.get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok", "version": "0.1.0"}


@router.post("/scrape")
async def trigger_scrape(request: ScrapeRequest) -> dict[str, str]:
    """Manually trigger the job scraping pipeline."""
    task = await scrape_job_page.kiq(request.query)
    return {"task_id": task.task_id, "status": "queued", "query": request.query}


def create_app() -> FastAPI:
    """Factory function for creating a configured FastAPI application."""
    application = FastAPI(title="TalentStream", lifespan=lifespan)
    application.include_router(router)
    return application
