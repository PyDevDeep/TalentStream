import google.generativeai as genai
import structlog
from google.api_core.exceptions import ResourceExhausted, ServiceUnavailable
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.clients.llm.prompts import EXTRACTION_PROMPT
from app.config import get_settings

logger = structlog.get_logger()

_model = None


def _get_model():  # type: ignore[no-untyped-def]
    """Return the shared Gemini GenerativeModel, initializing it on first call."""
    global _model
    if _model is None:
        genai.configure(api_key=get_settings().gemini_api_key.get_secret_value())  # type: ignore[attr-defined]
        _model = genai.GenerativeModel(  # type: ignore[attr-defined]
            "gemini-2.5-flash",
            generation_config={"response_mime_type": "application/json"},
        )
    return _model


# Conservative limit: ~4 chars/token avg → 20 000 chars ≈ 5 000 tokens,
# well under the 8 000-token guard.
_MAX_TEXT_CHARS = 20_000


@retry(
    retry=retry_if_exception_type((ResourceExhausted, ServiceUnavailable)),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(3),
    reraise=True,
)
async def parse_with_gemini(text: str) -> str | None:
    """Parse job text with Google Gemini and return the raw JSON response string."""
    try:
        m = _get_model()
        token_count = m.count_tokens(text).total_tokens  # type: ignore[union-attr]
        if token_count > 8000:
            logger.warning("gemini_text_too_long", tokens=token_count)
            text = text[:_MAX_TEXT_CHARS]

        prompt = f"{EXTRACTION_PROMPT}\n\nJob Text:\n{text}"
        response = await m.generate_content_async(prompt)  # type: ignore[union-attr]
        return response.text
    except Exception as e:
        logger.error("gemini_parse_error", error=str(e))
        raise
