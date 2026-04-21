import google.generativeai as genai
import structlog
from google.api_core.exceptions import ResourceExhausted, ServiceUnavailable
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.clients.llm.prompts import EXTRACTION_PROMPT
from app.config import get_settings

logger = structlog.get_logger()
settings = get_settings()

genai.configure(api_key=settings.gemini_api_key.get_secret_value())  # type: ignore[attr-defined]
model = genai.GenerativeModel(  # type: ignore[attr-defined]
    "gemini-2.5-flash",
    generation_config={"response_mime_type": "application/json"},
)


@retry(
    retry=retry_if_exception_type((ResourceExhausted, ServiceUnavailable)),
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(3),
    reraise=True,
)
async def parse_with_gemini(text: str) -> str | None:
    """Парсинг тексту через Google Gemini."""
    try:
        token_count = model.count_tokens(text).total_tokens  # type: ignore[union-attr]
        if token_count > 8000:
            logger.warning("gemini_text_too_long", tokens=token_count)
            text = text[:30000]  # Жорстке усічення для запобігання помилок

        prompt = f"{EXTRACTION_PROMPT}\n\nJob Text:\n{text}"
        response = await model.generate_content_async(prompt)  # type: ignore[union-attr]
        return response.text
    except Exception as e:
        logger.error("gemini_parse_error", error=str(e))
        raise
