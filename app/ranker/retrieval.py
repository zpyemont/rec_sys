"""
Embedding-based candidate retrieval using pgvector.

This module replaces the heuristic candidate generation in candidate_sources.py
with embedding-based similarity search. It uses:
- pgvector for ANN (Approximate Nearest Neighbor) search
- Parallel execution for multiple retrieval sources
- Redis caching for fresh/trending candidates

The retrieval pipeline:
1. Compute user embedding from liked products
2. Run parallel retrievers (embedding, fresh, trending, random)
3. Merge and deduplicate results
4. Pass to Monolith for ranking
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

    This single query replaces the 5 heuristic SQL queries in candidate_sources.py.
    The pgvector <=> operator computes cosine distance efficiently using the HNSW index.

    Args:
        pg: Async PostgreSQL client
        user_embedding: 512-dim user embedding from get_user_embedding()
        shown_set: Set of product IDs already shown to user
        limit: Maximum number of candidates to return

    Returns:
        List of product IDs ordered by similarity to user embedding
    """
    # Convert shown_set to list, limit size to prevent query parameter overflow
    shown_list = list(shown_set)[:10000] if shown_set else []

    # pgvector <=> operator computes cosine distance (smaller = more similar)
    result = await pg.fetch_val_list("""
        SELECT product_id
        FROM products
        WHERE is_active = true
          AND availability NOT IN ('out of stock', 'sold out')
          AND image_embedding IS NOT NULL
          AND ($1::text[] IS NULL OR product_id != ALL($1::text[]))
        ORDER BY image_embedding <=> $2::vector
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

    Fresh products are those added/parsed in the last 48 hours,
    important for discovery and keeping the feed dynamic.

    Args:
        redis: Async Redis client for caching
        pg: Async PostgreSQL client
        limit: Maximum number of candidates to return

    Returns:
        List of product IDs ordered by recency
    """
    cache_key = "candidates:fresh"

    # Try cache first
    cached = await redis.get(cache_key)
    if cached:
        logger.debug(f"Fresh candidates cache hit ({len(cached)} items)")
        return cached[:limit]

    # Cache miss - query PostgreSQL
    logger.debug("Fresh candidates cache miss, querying PostgreSQL")

    fresh = await pg.fetch_val_list("""
        SELECT product_id
        FROM products
        WHERE is_active = true
          AND availability NOT IN ('out of stock', 'sold out')
          AND parsed_at >= NOW() - INTERVAL '48 hours'
        ORDER BY parsed_at DESC
        LIMIT $1
    """, [limit * 2])  # Fetch extra for variety

    # Cache result (TTL: 5 minutes)
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

    Trending products have high like_count and are recent (48 hours).
    This provides social proof in the feed.

    Args:
        redis: Async Redis client for caching
        pg: Async PostgreSQL client
        limit: Maximum number of candidates to return

    Returns:
        List of product IDs ordered by engagement
    """
    cache_key = "candidates:trending"

    # Try cache first
    cached = await redis.get(cache_key)
    if cached:
        logger.debug(f"Trending candidates cache hit ({len(cached)} items)")
        return cached[:limit]

    # Cache miss - query PostgreSQL
    logger.debug("Trending candidates cache miss, querying PostgreSQL")

    trending = await pg.fetch_val_list("""
        SELECT product_id
        FROM products
        WHERE is_active = true
          AND availability NOT IN ('out of stock', 'sold out')
          AND like_count > 5
          AND parsed_at >= NOW() - INTERVAL '48 hours'
        ORDER BY like_count DESC, parsed_at DESC
        LIMIT $1
    """, [limit * 2])

    # Cache result (TTL: 5 minutes)
    if trending:
        await redis.setex(cache_key, 300, trending)

    return trending[:limit]


async def get_random_candidates(
    pg: "AsyncPostgresClient",
    limit: int = 50,
) -> List[str]:
    """
    Get random high-quality products for exploration.

    Uses TABLESAMPLE for efficient random selection (not ORDER BY RANDOM).
    Only includes products with like_count >= 3 for quality.

    Args:
        pg: Async PostgreSQL client
        limit: Maximum number of candidates to return

    Returns:
        List of randomly sampled product IDs
    """
    # TABLESAMPLE is much faster than ORDER BY RANDOM()
    # BERNOULLI(1) samples ~1% of rows, adjust if needed
    result = await pg.fetch_val_list("""
        SELECT product_id
        FROM products TABLESAMPLE BERNOULLI(1)
        WHERE is_active = true
          AND availability NOT IN ('out of stock', 'sold out')
          AND like_count >= 3
        LIMIT $1
    """, [limit])

    # Fallback if TABLESAMPLE returns too few results
    if len(result) < limit // 2:
        logger.debug("TABLESAMPLE returned few results, using random_bucket fallback")
        # Use pre-computed random_bucket column
        bucket = np.random.randint(0, 100)
        result = await pg.fetch_val_list("""
            SELECT product_id
            FROM products
            WHERE is_active = true
              AND availability NOT IN ('out of stock', 'sold out')
              AND like_count >= 3
              AND random_bucket = $1
            LIMIT $2
        """, [bucket, limit])

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

    Args:
        pg: Async PostgreSQL client
        redis: Async Redis client
        user_embedding: 512-dim user embedding
        shown_set: Set of product IDs already shown to user
        total_limit: Maximum total candidates to return

    Returns:
        Merged and deduplicated list of product IDs

    Example:
        ```python
        user_emb = await get_user_embedding(pg, liked_ids)
        candidates = await get_candidates_parallel(pg, redis, user_emb, shown_set)
        # candidates is a list of ~500 product IDs
        ```
    """
    # Calculate limits for each retriever
    embedding_limit = int(total_limit * 0.70)
    fresh_limit = int(total_limit * 0.15)
    trending_limit = int(total_limit * 0.10)
    random_limit = int(total_limit * 0.05)

    logger.info(f"Running parallel retrieval: emb={embedding_limit}, fresh={fresh_limit}, "
                f"trending={trending_limit}, random={random_limit}")

    # Run ALL retrievers in parallel
    results = await asyncio.gather(
        get_embedding_candidates(pg, user_embedding, shown_set, embedding_limit),
        get_fresh_candidates(redis, pg, fresh_limit),
        get_trending_candidates(redis, pg, trending_limit),
        get_random_candidates(pg, random_limit),
        return_exceptions=True,  # Don't fail if one retriever fails
    )

    # Handle any exceptions
    embedding_ids = results[0] if not isinstance(results[0], Exception) else []
    fresh_ids = results[1] if not isinstance(results[1], Exception) else []
    trending_ids = results[2] if not isinstance(results[2], Exception) else []
    random_ids = results[3] if not isinstance(results[3], Exception) else []

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            retriever_names = ["embedding", "fresh", "trending", "random"]
            logger.error(f"Retriever {retriever_names[i]} failed: {result}")

    # Merge and deduplicate (preserving order, embedding first for relevance)
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

    Uses fresh + trending + random without embedding similarity.

    Args:
        pg: Async PostgreSQL client
        redis: Async Redis client
        total_limit: Maximum total candidates to return

    Returns:
        List of product IDs
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

    # Merge and deduplicate
    seen: Set[str] = set()
    merged: List[str] = []

    for pid in fresh_ids + trending_ids + random_ids:
        if pid not in seen:
            seen.add(pid)
            merged.append(pid)

    return merged[:total_limit]
