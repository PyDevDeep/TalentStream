import openai
import structlog
from openai import AsyncOpenAI
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.clients.llm.prompts import EXTRACTION_PROMPT

logger = structlog.get_logger()


class OpenAIClient:
    def __init__(self, api_key: str):
        self.client = AsyncOpenAI(api_key=api_key)

    @retry(
        retry=retry_if_exception_type(
            (openai.RateLimitError, openai.APIConnectionError, openai.InternalServerError)
        ),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    async def parse(self, text: str) -> str | None:
        try:
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a job parser. Return JSON only."},
                    {"role": "user", "content": f"{EXTRACTION_PROMPT}\n\n{text[:20000]}"},
                ],
                response_format={"type": "json_object"},
                temperature=0,
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error("openai_parsing_failed", error=str(e))
            raise
