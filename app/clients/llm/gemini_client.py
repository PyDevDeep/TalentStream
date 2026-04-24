import google.generativeai as genai
import structlog
from google.api_core.exceptions import ResourceExhausted, ServiceUnavailable
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.clients.llm.prompts import EXTRACTION_PROMPT

logger = structlog.get_logger()


class GeminiClient:
    def __init__(self, api_key: str) -> None:
        genai.configure(api_key=api_key)  # type: ignore[attr-defined]
        self.model = genai.GenerativeModel(  # type: ignore[attr-defined]
            "gemini-2.5-flash",
            generation_config={"response_mime_type": "application/json"},
        )
        self._max_text_chars = 20_000

    @retry(
        retry=retry_if_exception_type((ResourceExhausted, ServiceUnavailable)),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    async def parse(self, text: str) -> str | None:
        try:
            text_safe = text[: self._max_text_chars]

            response = await self.model.generate_content_async(  # type: ignore
                f"You are a job parser. Return JSON only.\n\n{EXTRACTION_PROMPT}\n\n{text_safe}"
            )
            return str(response.text) if response.text else None
        except Exception as e:
            logger.error("gemini_parsing_failed", error=str(e))
            raise
