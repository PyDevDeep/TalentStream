import structlog

from app.clients.llm.base import LLMClientProtocol

logger = structlog.get_logger()


class LLMRouter:
    def __init__(self, primary_client: LLMClientProtocol, fallback_client: LLMClientProtocol):
        self.primary = primary_client
        self.fallback = fallback_client

    async def extract_job_data(self, text: str) -> str | None:
        """Спробувати первинний клієнт, при невдачі — fallback."""
        try:
            logger.info("llm_router_trying_primary")
            return await self.primary.parse(text)
        except Exception as e:
            logger.warning("llm_router_primary_failed", error=str(e))
            try:
                logger.info("llm_router_trying_fallback")
                return await self.fallback.parse(text)
            except Exception as fallback_err:
                logger.error(
                    "llm_router_all_failed", primary_error=str(e), fallback_error=str(fallback_err)
                )
                return None
