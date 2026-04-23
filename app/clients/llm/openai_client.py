"""
File: app/clients/llm/openai_client.py
Task: 1.2.1 - Implement LLM Clients
Dependencies: openai, tenacity, app.config
"""

import structlog
from openai import APIError, AsyncOpenAI, RateLimitError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.clients.llm.prompts import EXTRACTION_PROMPT
from app.config import get_settings

logger = structlog.get_logger()

_client: AsyncOpenAI | None = None


def _get_client() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(api_key=get_settings().openai_api_key.get_secret_value())
    return _client


@retry(
    retry=retry_if_exception_type((RateLimitError, APIError)),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    stop=stop_after_attempt(3),
    reraise=True,
)
async def parse_with_openai(text: str) -> str | None:
    """Парсинг тексту через OpenAI."""
    try:
        response = await _get_client().chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": EXTRACTION_PROMPT},
                {"role": "user", "content": f"Job Text:\n{text}"},
            ],
            response_format={"type": "json_object"},
            temperature=0.0,
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error("openai_parse_error", error=str(e))
        raise
