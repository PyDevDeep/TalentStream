import hashlib

from redis.asyncio import Redis


class DedupService:
    def __init__(self, redis_client: Redis, ttl: int = 86400):
        """
        Ініціалізація сервісу з клієнтом Redis та часом життя ключа.
        За замовчуванням TTL = 86400 секунд (24 години).
        """
        self.redis = redis_client
        self.ttl = ttl

    def _make_key(self, url: str) -> str:
        """Генерує унікальний ключ Redis на основі SHA-256 хешу URL."""
        url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return f"job:seen:{url_hash}"

    async def is_duplicate(self, url: str) -> bool:
        """
        Атомарна перевірка та запис (Check-and-Set).
        Повертає True, якщо URL вже оброблявся.
        """
        key = self._make_key(url)
        # nx=True гарантує, що запис відбудеться ТІЛЬКИ якщо ключа немає.
        # Повертає True при успішному записі, або None, якщо ключ існує.
        result = await self.redis.set(key, 1, nx=True, ex=self.ttl)
        return result is None
