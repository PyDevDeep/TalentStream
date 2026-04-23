"""
Unit tests for app/services/dedup.py.

Coverage targets:
- DedupService.is_duplicate(): first call, second call, different URLs, concurrent calls
Uses fakeredis.aioredis for in-memory Redis without a real server.
"""

import asyncio
from collections.abc import AsyncGenerator

import fakeredis.aioredis
import pytest

from app.services.dedup import DedupService

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def redis_client() -> AsyncGenerator[fakeredis.aioredis.FakeRedis, None]:
    """In-memory async Redis client, flushed between tests."""
    client = fakeredis.aioredis.FakeRedis()
    yield client
    await client.aclose()


@pytest.fixture
def dedup(redis_client: fakeredis.aioredis.FakeRedis) -> DedupService:
    """DedupService backed by fakeredis."""
    return DedupService(redis_client=redis_client, ttl=3600)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDedupService:
    """Tests for DedupService.is_duplicate()."""

    @pytest.mark.asyncio
    async def test_first_call_not_duplicate(self, dedup: DedupService) -> None:
        """First time a URL is seen → is_duplicate returns False."""
        result = await dedup.is_duplicate("https://example.com/job/1")
        assert result is False

    @pytest.mark.asyncio
    async def test_second_call_is_duplicate(self, dedup: DedupService) -> None:
        """Same URL called twice → second call returns True."""
        url = "https://example.com/job/2"
        first = await dedup.is_duplicate(url)
        second = await dedup.is_duplicate(url)
        assert first is False
        assert second is True

    @pytest.mark.asyncio
    async def test_different_urls_not_duplicate(self, dedup: DedupService) -> None:
        """Two distinct URLs → both return False independently."""
        url_a = "https://example.com/job/3"
        url_b = "https://example.com/job/4"
        assert await dedup.is_duplicate(url_a) is False
        assert await dedup.is_duplicate(url_b) is False

    @pytest.mark.asyncio
    async def test_multiple_subsequent_calls_all_duplicate(self, dedup: DedupService) -> None:
        """After first insertion, all subsequent calls for same URL return True."""
        url = "https://example.com/job/5"
        await dedup.is_duplicate(url)
        for _ in range(3):
            assert await dedup.is_duplicate(url) is True

    @pytest.mark.asyncio
    async def test_concurrent_calls_only_one_not_duplicate(self, dedup: DedupService) -> None:
        """Concurrent calls for same URL → exactly one False, rest True (atomic SET NX)."""
        url = "https://example.com/job/concurrent"
        results = await asyncio.gather(*[dedup.is_duplicate(url) for _ in range(5)])
        false_count = results.count(False)
        true_count = results.count(True)
        assert false_count == 1, f"Expected exactly 1 non-duplicate, got {false_count}"
        assert true_count == 4

    @pytest.mark.asyncio
    async def test_key_uses_sha256_hash(self, dedup: DedupService) -> None:
        """Internal key is derived from SHA256, not raw URL."""
        import hashlib

        url = "https://example.com/job/hash-test"
        await dedup.is_duplicate(url)

        expected_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
        expected_key = f"job:seen:{expected_hash}"

        exists = await dedup.redis.exists(expected_key)
        assert exists == 1

    @pytest.mark.asyncio
    async def test_ttl_is_set_on_key(self, dedup: DedupService) -> None:
        """Redis key has a TTL set after first call."""
        url = "https://example.com/job/ttl-test"
        await dedup.is_duplicate(url)

        key = dedup._make_key(url)
        ttl = await dedup.redis.ttl(key)
        assert ttl > 0, "TTL should be positive after key creation"
