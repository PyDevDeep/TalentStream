from typing import Protocol


class LLMClientProtocol(Protocol):
    async def parse(self, text: str) -> str | None:
        """Parse job text and return JSON string."""
        ...
