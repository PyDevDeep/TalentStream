from app.broker import broker


@broker.task(task_name="scrape_job_page")
async def scrape_job_page(query: str) -> dict[str, object]:
    """Stub for job scraping. Will be implemented in Phase 3."""
    return {}


@broker.task(task_name="send_alert")
async def send_alert() -> dict[str, object]:
    """Stub for sending alerts. Will be implemented in Phase 3."""
    return {}


__all__ = ["scrape_job_page", "send_alert"]
