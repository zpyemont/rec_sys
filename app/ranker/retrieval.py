"""
Embedding-based candidate retrieval using pgvector (v2 - normalized schema).

Supports two embedding types:
- CLIP image embeddings (512-dim) - legacy
- Two-tower learned embeddings (128-dim) - preferred when available

Uses the new schema structure with JOINs across:
- catalog.products
- catalog.product_pricing
- embeddings.product_vectors
"""

from __future__ import annotations

import asyncio
import logging
from typing import List, Set, TYPE_CHECKING

import numpy as np

from ..settings import Settings

if TYPE_CHECKING:
    from ..connectors.postgres import AsyncPostgresClient
    from ..connectors.redis_client import AsyncRedisClient

logger = logging.getLogger(__name__)


def _availability_filter(param_idx: int) -> tuple[str, list, int]:
    """Build stock availability filter clause for asyncpg queries.

    Returns (sql_fragment, params, next_param_idx).
    Uses settings.excluded_availability_values with case-insensitive matching.
    Products with NULL availability are included (not filtered out).
    """
    s = Settings()
    if not s.filter_out_of_stock or not s.excluded_availability_values:
        return "", [], param_idx
    excluded = [v.lower() for v in s.excluded_availability_values]
    clause = f"AND (pr.availability IS NULL OR LOWER(pr.availability) != ALL(${param_idx}::text[]))"
    return clause, [excluded], param_idx + 1


async def get_embedding_candidates(
    pg: "AsyncPostgresClient",
    user_embedding: np.ndarray,
    shown_set: Set[str],
    limit: int = 500,
    use_learned: bool = False,
) -> List[str]:
    """
    Retrieve candidates using embedding similarity via pgvector.

    Args:
        pg: Async PostgreSQL client (v2)
        user_embedding: User embedding (128-dim learned or 512-dim CLIP)
        shown_set: Set of product IDs already shown to user
        limit: Maximum number of candidates to return
        use_learned: If True, search against learned_embedding (128-dim two-tower);
                     otherwise use image_embedding (512-dim CLIP)

    Returns:
        List of product IDs ordered by similarity to user embedding
    """
    shown_list = list(shown_set)[:10000] if shown_set else []

    emb_col = "learned_embedding" if use_learned else "image_embedding"

    avail_clause, avail_params, _ = _availability_filter(4)

    result = await pg.fetch_val_list(f"""
        SELECT e.product_id
        FROM embeddings.product_vectors e
        JOIN catalog.product_pricing pr ON e.product_id = pr.product_id
        WHERE pr.is_active = true
          {avail_clause}
          AND e.{emb_col} IS NOT NULL
          AND ($1::text[] IS NULL OR e.product_id != ALL($1::text[]))
        ORDER BY e.{emb_col} <=> $2::vector
        LIMIT $3
    """, [shown_list if shown_list else None, user_embedding.tolist(), limit] + avail_params)

    logger.debug(f"Retrieved {len(result)} embedding candidates (col={emb_col})")
    return result


async def get_fresh_candidates(
    redis: "AsyncRedisClient",
    pg: "AsyncPostgresClient",
    limit: int = 150,
    max_per_brand: int = 5,
) -> List[str]:
    """
    Get fresh products with brand diversity (cached in Redis, refreshed every 5 min).
    Uses ROW_NUMBER PARTITION BY brand to cap each brand at max_per_brand items.
    """
    cache_key = "candidates:fresh:v3"

    cached = await redis.get(cache_key)
    if cached:
        logger.debug(f"Fresh candidates cache hit ({len(cached)} items)")
        return cached[:limit]

    logger.debug("Fresh candidates cache miss, querying PostgreSQL")

    avail_clause, avail_params, _ = _availability_filter(3)

    fresh = await pg.fetch_val_list(f"""
        WITH ranked AS (
            SELECT p.product_id,
                   ROW_NUMBER() OVER (PARTITION BY p.brand ORDER BY p.parsed_at DESC) AS rn
            FROM catalog.products p
            JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
            WHERE pr.is_active = true
              {avail_clause}
              AND p.parsed_at >= NOW() - INTERVAL '7 days'
        )
        SELECT product_id FROM ranked
        WHERE rn <= $1
        ORDER BY rn, RANDOM()
        LIMIT $2
    """, [max_per_brand, limit * 2] + avail_params)

    if fresh:
        await redis.setex(cache_key, 300, fresh)

    return fresh[:limit]


async def get_trending_candidates(
    redis: "AsyncRedisClient",
    pg: "AsyncPostgresClient",
    limit: int = 100,
    max_per_brand: int = 5,
) -> List[str]:
    """
    Get trending products with brand diversity (cached in Redis, refreshed every 5 min).
    Uses ROW_NUMBER PARTITION BY brand to cap each brand.
    """
    cache_key = "candidates:trending:v3"

    cached = await redis.get(cache_key)
    if cached:
        logger.debug(f"Trending candidates cache hit ({len(cached)} items)")
        return cached[:limit]

    logger.debug("Trending candidates cache miss, querying PostgreSQL")

    avail_clause, avail_params, _ = _availability_filter(3)

    trending = await pg.fetch_val_list(f"""
        WITH ranked AS (
            SELECT p.product_id,
                   ROW_NUMBER() OVER (PARTITION BY p.brand ORDER BY pr.like_count DESC, p.parsed_at DESC) AS rn
            FROM catalog.products p
            JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
            WHERE pr.is_active = true
              {avail_clause}
              AND pr.like_count >= 0
              AND p.parsed_at >= NOW() - INTERVAL '7 days'
        )
        SELECT product_id FROM ranked
        WHERE rn <= $1
        ORDER BY rn, RANDOM()
        LIMIT $2
    """, [max_per_brand, limit * 2] + avail_params)

    if trending:
        await redis.setex(cache_key, 300, trending)

    return trending[:limit]


async def get_random_candidates(
    pg: "AsyncPostgresClient",
    limit: int = 50,
) -> List[str]:
    """
    Get random high-quality products for exploration.
    """
    avail_clause, avail_params, _ = _availability_filter(2)

    result = await pg.fetch_val_list(f"""
        SELECT p.product_id
        FROM catalog.products p
        TABLESAMPLE BERNOULLI(1)
        JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
        WHERE pr.is_active = true
          {avail_clause}
        LIMIT $1
    """, [limit] + avail_params)

    if len(result) < limit // 2:
        logger.debug("TABLESAMPLE returned few results, using fallback")
        result = await pg.fetch_val_list(f"""
            SELECT p.product_id
            FROM catalog.products p
            JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
            WHERE pr.is_active = true
              {avail_clause}
            ORDER BY RANDOM()
            LIMIT $1
        """, [limit] + avail_params)

    logger.debug(f"Retrieved {len(result)} random candidates")
    return result


async def get_candidates_parallel(
    pg: "AsyncPostgresClient",
    redis: "AsyncRedisClient",
    user_embedding: np.ndarray,
    shown_set: Set[str],
    total_limit: int = 500,
    use_learned: bool = False,
) -> List[str]:
    """
    Run all retrievers in parallel and merge results.

    Distribution:
    - 70% embedding similarity (personalized)
    - 15% fresh products (discovery)
    - 10% trending products (social proof)
    - 5% random (exploration)
    """
    embedding_limit = int(total_limit * 0.70)
    fresh_limit = int(total_limit * 0.15)
    trending_limit = int(total_limit * 0.10)
    random_limit = int(total_limit * 0.05)

    logger.info(f"Running parallel retrieval: emb={embedding_limit}, fresh={fresh_limit}, "
                f"trending={trending_limit}, random={random_limit}")

    results = await asyncio.gather(
        get_embedding_candidates(pg, user_embedding, shown_set, embedding_limit, use_learned=use_learned),
        get_fresh_candidates(redis, pg, fresh_limit),
        get_trending_candidates(redis, pg, trending_limit),
        get_random_candidates(pg, random_limit),
        return_exceptions=True,
    )

    embedding_ids = results[0] if not isinstance(results[0], Exception) else []
    fresh_ids = results[1] if not isinstance(results[1], Exception) else []
    trending_ids = results[2] if not isinstance(results[2], Exception) else []
    random_ids = results[3] if not isinstance(results[3], Exception) else []

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            retriever_names = ["embedding", "fresh", "trending", "random"]
            logger.error(f"Retriever {retriever_names[i]} failed: {result}")

    seen: Set[str] = set()
    merged: List[str] = []

    for pid in embedding_ids + fresh_ids + trending_ids + random_ids:
        if pid not in seen and pid not in shown_set:
            seen.add(pid)
            merged.append(pid)

    logger.info(f"Merged {len(merged)} unique candidates from: "
                f"emb={len(embedding_ids)}, fresh={len(fresh_ids)}, "
                f"trending={len(trending_ids)}, random={len(random_ids)}")

    return merged[:total_limit]


async def get_candidates_for_anonymous(
    pg: "AsyncPostgresClient",
    redis: "AsyncRedisClient",
    total_limit: int = 500,
) -> List[str]:
    """
    Get candidates for anonymous users (no personalization).
    """
    fresh_limit = int(total_limit * 0.50)
    trending_limit = int(total_limit * 0.35)
    random_limit = int(total_limit * 0.15)

    results = await asyncio.gather(
        get_fresh_candidates(redis, pg, fresh_limit),
        get_trending_candidates(redis, pg, trending_limit),
        get_random_candidates(pg, random_limit),
        return_exceptions=True,
    )

    fresh_ids = results[0] if not isinstance(results[0], Exception) else []
    trending_ids = results[1] if not isinstance(results[1], Exception) else []
    random_ids = results[2] if not isinstance(results[2], Exception) else []

    seen: Set[str] = set()
    merged: List[str] = []

    for pid in fresh_ids + trending_ids + random_ids:
        if pid not in seen:
            seen.add(pid)
            merged.append(pid)

    return merged[:total_limit]


async def get_candidates_explore_exploit(
    pg: "AsyncPostgresClient",
    redis: "AsyncRedisClient",
    exploit_candidates: List[str],
    epsilon: float = 0.2,
    total_limit: int = 500,
) -> List[str]:
    """
    Merge exploit (personalized) candidates with explore (diverse discovery) candidates.

    Explore candidates are brand-diverse trending items that add variety to the feed.
    The epsilon parameter controls what fraction of the feed is exploration.

    Args:
        pg: Async PostgreSQL client
        redis: Async Redis client
        exploit_candidates: Personalized/ranked candidate IDs
        epsilon: Fraction of feed for exploration (0.0 = all exploit, 1.0 = all explore)
        total_limit: Total candidates to return

    Returns:
        Merged candidate list with explore items interleaved
    """
    explore_count = int(total_limit * epsilon)
    exploit_count = total_limit - explore_count

    # Get explore candidates: brand-diverse trending across all categories
    explore_candidates = await get_trending_candidates(redis, pg, limit=explore_count * 2)

    # Remove any overlap with exploit set
    exploit_set = set(exploit_candidates)
    explore_candidates = [pid for pid in explore_candidates if pid not in exploit_set][:explore_count]

    # Interleave: every 1/epsilon items, insert an explore item
    exploit_pool = exploit_candidates[:exploit_count]
    merged: List[str] = []
    explore_idx = 0
    step = max(1, int(1.0 / epsilon)) if epsilon > 0 else total_limit + 1

    for i, pid in enumerate(exploit_pool):
        merged.append(pid)
        if (i + 1) % step == 0 and explore_idx < len(explore_candidates):
            merged.append(explore_candidates[explore_idx])
            explore_idx += 1

    # Append remaining explore items
    while explore_idx < len(explore_candidates):
        merged.append(explore_candidates[explore_idx])
        explore_idx += 1

    # Deduplicate while preserving order
    seen: Set[str] = set()
    result: List[str] = []
    for pid in merged:
        if pid not in seen:
            seen.add(pid)
            result.append(pid)

    return result[:total_limit]
