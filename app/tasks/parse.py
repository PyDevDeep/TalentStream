import json

import structlog
from pydantic import ValidationError
from taskiq import Context, TaskiqDepends

from app.broker import broker
from app.clients import LLMRouter, SerperClient
from app.config import get_settings
from app.db.repository import JobRepository
from app.db.session import get_session
from app.schemas.job import ParsedJob
from app.services import DedupService, FilterEngine, strip_noise

logger = structlog.get_logger()


@broker.task(task_name="parse_job")
async def parse_job(
    url: str,
    context: Context = TaskiqDepends(),  # noqa: B008
) -> dict[str, object]:
    """Run the full pipeline: dedup → fetch → clean → LLM parse → filter → DB store."""
    log = logger.bind(url=url)
    log.info("parse_job_started")

    settings = get_settings()

    # --- Dedup (shared Redis from TaskiqState) ---
    redis_client = context.state.redis_client
    dedup = DedupService(redis_client, ttl=settings.dedup_ttl_seconds)
    if await dedup.is_duplicate(url):
        log.info("job_skipped_duplicate_redis")
        return {"status": "duplicate", "job_id": None}

    # --- Fetch page content ---
    serper = SerperClient(api_key=settings.serper_api_key.get_secret_value())
    try:
        raw_text = await serper.view(url=url)
        if not raw_text:
            log.warning("empty_page_content")
            return {"status": "error", "job_id": None}
    except Exception as e:
        log.error("serper_view_failed", error=str(e))
        return {"status": "error", "job_id": None}
    finally:
        await serper.close()

    # --- Clean noise ---
    cleaned_text = strip_noise(raw_text)

    # --- LLM extraction ---
    raw_json = await LLMRouter.extract_job_data(cleaned_text)
    if not raw_json:
        log.error("llm_parsing_failed")
        return {"status": "error", "job_id": None}

    # --- Validate ---
    try:
        raw_dict = json.loads(raw_json)
        raw_dict["url"] = url
        parsed_job = ParsedJob.model_validate(raw_dict)
    except (ValidationError, json.JSONDecodeError) as e:
        log.error("validation_failed", error=str(e), raw_json=raw_json)
        return {"status": "error", "job_id": None}

    # --- Filter ---
    filter_engine = FilterEngine(
        keywords=settings.filter_keywords,
        location=settings.filter_location,
        salary_min=settings.filter_salary_min,
    )
    if not filter_engine.passes(parsed_job):
        log.info("job_skipped_filtered")
        return {"status": "filtered", "job_id": None}

    # --- Store ---
    async with get_session() as session:
        repo = JobRepository(session)
        job = await repo.upsert(parsed_job)

        if not job:
            log.info("job_duplicate_in_db")
            return {"status": "duplicate_db", "job_id": None}

        log.info("job_stored_successfully", job_id=job.id)
        return {"status": "stored", "job_id": job.id}
