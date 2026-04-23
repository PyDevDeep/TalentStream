import structlog

from app.broker import broker
from app.clients import SerperClient
from app.config import get_settings
from app.tasks.parse import parse_job

logger = structlog.get_logger()


@broker.task(task_name="scrape_job_page")
async def scrape_job_page(query: str) -> dict[str, int]:
    """Search for jobs via Serper and enqueue a parse task for each result URL."""
    logger.info("scrape_task_started", query=query)

    client = SerperClient(api_key=get_settings().serper_api_key.get_secret_value())
    urls_found = 0
    tasks_queued = 0

    try:
        urls = await client.search(query=query, num_results=3)
        urls_found = len(urls)

        for url in urls:
            await parse_job.kiq(url)
            tasks_queued += 1

    except Exception as e:
        logger.error("scrape_task_error", error=str(e), query=query)
        raise
    finally:
        await client.close()

    logger.info("scrape_task_completed", urls_found=urls_found, tasks_queued=tasks_queued)

    return {"urls_found": urls_found, "tasks_queued": tasks_queued}
