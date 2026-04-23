import structlog

from .gemini_client import parse_with_gemini
from .openai_client import parse_with_openai

logger = structlog.get_logger()


def clean_json_response(raw: str) -> str:
    """Strip markdown code fences from an LLM response."""
    raw = raw.strip()
    if raw.startswith("```json"):
        raw = raw[7:]
    if raw.startswith("```"):
        raw = raw[3:]
    if raw.endswith("```"):
        raw = raw[:-3]
    return raw.strip()


class LLMRouter:
    @staticmethod
    async def extract_job_data(text: str) -> str | None:
        """Extract structured job data using OpenAI (primary) with Gemini as fallback. Returns raw JSON or None."""
        try:
            logger.info("llm_router_trying_openai")
            result = await parse_with_openai(text)
            if result:
                return clean_json_response(result)
        except Exception as e:
            logger.warning("llm_router_openai_failed", error=str(e), action="fallback_to_gemini")

        try:
            logger.info("llm_router_trying_gemini")
            result = await parse_with_gemini(text)
            if result:
                return clean_json_response(result)
        except Exception as e:
            logger.error("llm_router_all_models_failed", error=str(e))

        return None
