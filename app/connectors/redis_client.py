from __future__ import annotations

import json
import logging
from typing import Iterable, Set, List, Optional, Any, TYPE_CHECKING

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # fallback if not installed in some env

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None

if TYPE_CHECKING:
    from ..settings import Settings

logger = logging.getLogger(__name__)


class AsyncRedisClient:
    """
    Async Redis client for caching candidate lists.

    Used by the retrieval module to cache fresh/trending candidates
    with a 5-minute TTL to reduce PostgreSQL load.
    """

    def __init__(self, url: str):
        if aioredis is None:
            raise RuntimeError("redis.asyncio not available. Install with: pip install redis>=4.0")
        self._url = url
        self._pool: Optional[Any] = None

    @classmethod
    def from_settings(cls, settings: "Settings") -> Optional["AsyncRedisClient"]:
        """Create client from settings, returns None if Redis not configured."""
        if not settings.redis_url:
            logger.warning("Redis URL not configured, caching disabled")
            return None
        return cls(settings.redis_url)

    async def _get_pool(self):
        """Lazy-initialize the connection pool."""
        if self._pool is None:
            self._pool = aioredis.from_url(
                self._url,
                encoding="utf-8",
                decode_responses=True
            )
        return self._pool

    async def get(self, key: str) -> Optional[List[str]]:
        """
        Get a cached list of product IDs.

        Args:
            key: Cache key (e.g., "candidates:fresh")

        Returns:
            List of product IDs if cached, None otherwise
        """
        try:
            pool = await self._get_pool()
            value = await pool.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.warning(f"Redis get failed for {key}: {e}")
            return None

    async def setex(self, key: str, ttl_seconds: int, value: List[str]) -> bool:
        """
        Set a cached list of product IDs with TTL.

        Args:
            key: Cache key (e.g., "candidates:fresh")
            ttl_seconds: Time-to-live in seconds
            value: List of product IDs to cache

        Returns:
            True if successful, False otherwise
        """
        try:
            pool = await self._get_pool()
            await pool.setex(key, ttl_seconds, json.dumps(value))
            return True
        except Exception as e:
            logger.warning(f"Redis setex failed for {key}: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """Delete a cache key."""
        try:
            pool = await self._get_pool()
            await pool.delete(key)
            return True
        except Exception as e:
            logger.warning(f"Redis delete failed for {key}: {e}")
            return False

    async def close(self):
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None


class NoOpAsyncRedisClient:
    """
    No-op Redis client for when Redis is not configured.

    All get() calls return None (cache miss), setex() calls are ignored.
    """

    async def get(self, key: str) -> None:
        return None

    async def setex(self, key: str, ttl_seconds: int, value: List[str]) -> bool:
        return True

    async def delete(self, key: str) -> bool:
        return True

    async def close(self):
        pass


def get_async_redis_client(settings: "Settings") -> AsyncRedisClient | NoOpAsyncRedisClient:
    """
    Get async Redis client, or no-op client if not configured.

    Returns a no-op client instead of None so callers don't need null checks.
    """
    client = AsyncRedisClient.from_settings(settings)
    if client is None:
        logger.info("Using NoOpAsyncRedisClient (Redis not configured)")
        return NoOpAsyncRedisClient()
    return client


# ============================================
# Synchronous Redis functions (existing code)
# ============================================


def get_redis_client_safe(settings: "Settings"):
    if not settings.redis_url or not redis:
        return None
    try:
        client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
        # Ping to validate
        client.ping()
        return client
    except Exception:
        return None


def get_shown_set_safe(redis_client, user_id: str) -> Set[str]:
    if not redis_client:
        return set()
    try:
        members = redis_client.smembers(f"user_seen:{user_id}")
        return set(str(m) for m in members)
    except Exception:
        return set()


def add_shown_items_safe(redis_client, user_id: str, prod_ids: Iterable[str]) -> None:
    if not redis_client:
        return
    try:
        if not prod_ids:
            return
        redis_client.sadd(f"user_seen:{user_id}", *list(prod_ids))
    except Exception:
        # Swallow errors in mock stage
        return
