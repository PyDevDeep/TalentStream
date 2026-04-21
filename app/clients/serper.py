import httpx
import structlog
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

logger = structlog.get_logger()


def _is_retryable(exc: BaseException) -> bool:
    """Визначає, чи підлягає виняток повторній спробі."""
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or status >= 500
    return False


class SerperClient:
    def __init__(self, api_key: str, timeout: float = 30.0):
        self.headers = {
            "X-API-KEY": api_key,
            "Content-Type": "application/json",
        }
        self.client = httpx.AsyncClient(
            headers=self.headers,
            timeout=timeout,
            http2=True,
        )

    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        reraise=True,
    )
    async def search(self, query: str, num_results: int = 10) -> list[str]:
        """Пошук вакансій через Serper Search API."""
        response = await self.client.post(
            "https://google.serper.dev/search",
            json={"q": query, "num": num_results},
        )
        response.raise_for_status()
        data = response.json()

        urls = [item["link"] for item in data.get("organic", []) if "link" in item]
        logger.info("serper_search_success", query=query, found=len(urls))
        return urls

    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        reraise=True,
    )
    async def view(self, url: str) -> str:
        """Fetch raw content сторінки через Serper View API."""
        response = await self.client.post(
            "https://google.serper.dev/view",
            json={"url": url},
        )
        response.raise_for_status()
        data = response.json()

        text_content = data.get("text", "")
        if not text_content:
            logger.warning("serper_view_empty_content", url=url)

        logger.info("serper_view_success", url=url, text_length=len(text_content))
        return text_content

    async def close(self) -> None:
        """Graceful shutdown httpx client."""
        await self.client.aclose()
