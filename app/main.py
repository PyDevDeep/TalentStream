from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
from fastapi import FastAPI

from app.scheduler import SCHEDULED_TASKS, redis_source

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    await redis_source.startup()
    for task in SCHEDULED_TASKS:
        await redis_source.add_schedule(task)
    logger.info("Schedules registered in Redis", task_count=len(SCHEDULED_TASKS))

    yield

    await redis_source.shutdown()
    logger.info("Redis schedule source disconnected")


app = FastAPI(title="TalentStream", lifespan=lifespan)
