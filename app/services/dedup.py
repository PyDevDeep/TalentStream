import hashlib

from redis.asyncio import Redis


class DedupService:
    def __init__(self, redis_client: Redis, ttl: int = 86400):
        """Initialize with a Redis client and key TTL in seconds (default 24 h)."""
        self.redis = redis_client
        self.ttl = ttl

    def _make_key(self, url: str) -> str:
        """Build a namespaced Redis key from the SHA-256 hash of the URL."""
        url_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
        return f"job:seen:{url_hash}"

    async def is_duplicate(self, url: str) -> bool:
        """Atomically check-and-set the URL key. Returns True if already seen."""
        key = self._make_key(url)
        # SET NX returns True on first write, None if the key already exists.
        result = await self.redis.set(key, 1, nx=True, ex=self.ttl)
        return result is None
