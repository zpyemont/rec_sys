"""
Embedding-based candidate retrieval using pgvector (v2 - normalized schema).

Uses the new schema structure with JOINs across:
- catalog.products
- catalog.product_pricing
- embeddings.product_vectors
"""

from __future__ import annotations

import asyncio
import logging
from typing import List, Set, Optional, TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from ..connectors.postgres import AsyncPostgresClient
    from ..connectors.redis_client import AsyncRedisClient

logger = logging.getLogger(__name__)


async def get_embedding_candidates(
    pg: "AsyncPostgresClient",
    user_embedding: np.ndarray,
    shown_set: Set[str],
    limit: int = 500,
) -> List[str]:
    """
    Retrieve candidates using CLIP embedding similarity via pgvector.

    Args:
        pg: Async PostgreSQL client (v2)
        user_embedding: 512-dim user embedding
        shown_set: Set of product IDs already shown to user
        limit: Maximum number of candidates to return

    Returns:
        List of product IDs ordered by similarity to user embedding
    """
    shown_list = list(shown_set)[:10000] if shown_set else []

    result = await pg.fetch_val_list("""
        SELECT e.product_id
        FROM embeddings.product_vectors e
        JOIN catalog.product_pricing pr ON e.product_id = pr.product_id
        WHERE pr.is_active = true
          AND pr.availability NOT IN ('out of stock', 'sold out')
          AND e.image_embedding IS NOT NULL
          AND ($1::text[] IS NULL OR e.product_id != ALL($1::text[]))
        ORDER BY e.image_embedding <=> $2::vector
        LIMIT $3
    """, [shown_list if shown_list else None, user_embedding.tolist(), limit])

    logger.debug(f"Retrieved {len(result)} embedding candidates")
    return result


async def get_fresh_candidates(
    redis: "AsyncRedisClient",
    pg: "AsyncPostgresClient",
    limit: int = 150,
) -> List[str]:
    """
    Get fresh products (cached in Redis, refreshed every 5 min).
    """
    cache_key = "candidates:fresh:v2"

    cached = await redis.get(cache_key)
    if cached:
        logger.debug(f"Fresh candidates cache hit ({len(cached)} items)")
        return cached[:limit]

    logger.debug("Fresh candidates cache miss, querying PostgreSQL")

    fresh = await pg.fetch_val_list("""
        SELECT p.product_id
        FROM catalog.products p
        JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
        WHERE pr.is_active = true
          AND pr.availability NOT IN ('out of stock', 'sold out')
          AND p.parsed_at >= NOW() - INTERVAL '48 hours'
        ORDER BY p.parsed_at DESC
        LIMIT $1
    """, [limit * 2])

    if fresh:
        await redis.setex(cache_key, 300, fresh)

    return fresh[:limit]


async def get_trending_candidates(
    redis: "AsyncRedisClient",
    pg: "AsyncPostgresClient",
    limit: int = 100,
) -> List[str]:
    """
    Get trending products (cached in Redis, refreshed every 5 min).
    """
    cache_key = "candidates:trending:v2"

    cached = await redis.get(cache_key)
    if cached:
        logger.debug(f"Trending candidates cache hit ({len(cached)} items)")
        return cached[:limit]

    logger.debug("Trending candidates cache miss, querying PostgreSQL")

    trending = await pg.fetch_val_list("""
        SELECT p.product_id
        FROM catalog.products p
        JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
        WHERE pr.is_active = true
          AND pr.availability NOT IN ('out of stock', 'sold out')
          AND pr.like_count > 5
          AND p.parsed_at >= NOW() - INTERVAL '48 hours'
        ORDER BY pr.like_count DESC, p.parsed_at DESC
        LIMIT $1
    """, [limit * 2])

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
    result = await pg.fetch_val_list("""
        SELECT p.product_id
        FROM catalog.products p
        TABLESAMPLE BERNOULLI(1)
        JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
        WHERE pr.is_active = true
          AND pr.availability NOT IN ('out of stock', 'sold out')
          AND pr.like_count >= 3
        LIMIT $1
    """, [limit])

    if len(result) < limit // 2:
        logger.debug("TABLESAMPLE returned few results, using fallback")
        result = await pg.fetch_val_list("""
            SELECT p.product_id
            FROM catalog.products p
            JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
            WHERE pr.is_active = true
              AND pr.availability NOT IN ('out of stock', 'sold out')
              AND pr.like_count >= 3
            ORDER BY RANDOM()
            LIMIT $1
        """, [limit])

    logger.debug(f"Retrieved {len(result)} random candidates")
    return result


async def get_candidates_parallel(
    pg: "AsyncPostgresClient",
    redis: "AsyncRedisClient",
    user_embedding: np.ndarray,
    shown_set: Set[str],
    total_limit: int = 500,
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
        get_embedding_candidates(pg, user_embedding, shown_set, embedding_limit),
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
