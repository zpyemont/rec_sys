from typing import Iterable, Set, List

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # fallback if not installed in some env

from ..settings import Settings


def get_redis_client_safe(settings: Settings):
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
