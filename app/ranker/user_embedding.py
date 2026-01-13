"""
User embedding computation for personalized candidate retrieval.

This module computes user embeddings by averaging the CLIP embeddings
of products the user has liked. For cold-start users (no likes),
it falls back to a global average embedding.
"""

from __future__ import annotations

import logging
from typing import List, Optional, TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from ..connectors.postgres import AsyncPostgresClient

logger = logging.getLogger(__name__)

# Global average embedding for cold-start users (lazy-loaded)
_GLOBAL_AVG_EMBEDDING: Optional[np.ndarray] = None

# Embedding dimension (CLIP produces 512-dim embeddings)
EMBEDDING_DIM = 512


async def get_global_average_embedding(pg: "AsyncPostgresClient") -> np.ndarray:
    """
    Get the global average embedding across all products.

    This is used as a fallback for cold-start users who have no likes.
    The result is cached for subsequent calls.

    Args:
        pg: Async PostgreSQL client

    Returns:
        512-dim numpy array representing the average product embedding
    """
    global _GLOBAL_AVG_EMBEDDING

    if _GLOBAL_AVG_EMBEDDING is not None:
        return _GLOBAL_AVG_EMBEDDING

    logger.info("Computing global average embedding (one-time)")

    # pgvector supports AVG() on vector types
    result = await pg.fetch_all("""
        SELECT AVG(e.image_embedding)::float[] as avg_emb
        FROM embeddings.product_vectors e
        JOIN catalog.product_pricing pr ON e.product_id = pr.product_id
        WHERE e.image_embedding IS NOT NULL
          AND pr.is_active = true
    """)

    if result and result[0]['avg_emb']:
        _GLOBAL_AVG_EMBEDDING = np.array(result[0]['avg_emb'], dtype=np.float32)
        logger.info(f"Global average embedding computed: shape {_GLOBAL_AVG_EMBEDDING.shape}")
    else:
        # Fallback to zero vector if no embeddings exist
        logger.warning("No embeddings found, using zero vector as fallback")
        _GLOBAL_AVG_EMBEDDING = np.zeros(EMBEDDING_DIM, dtype=np.float32)

    return _GLOBAL_AVG_EMBEDDING


async def get_user_embedding(
    pg: "AsyncPostgresClient",
    liked_product_ids: List[str],
) -> np.ndarray:
    """
    Compute user embedding as the average of liked product CLIP embeddings.

    For users with no likes (cold-start), returns the global average embedding.
    This provides a reasonable starting point for recommendation.

    Args:
        pg: Async PostgreSQL client
        liked_product_ids: List of product IDs the user has liked

    Returns:
        512-dim numpy array representing the user's embedding

    Example:
        ```python
        liked_ids = ["prod_123", "prod_456", "prod_789"]
        user_emb = await get_user_embedding(pg, liked_ids)
        # user_emb.shape = (512,)
        ```
    """
    # Cold-start: no likes, use global average
    if not liked_product_ids:
        logger.debug("Cold-start user, using global average embedding")
        return await get_global_average_embedding(pg)

    # Fetch embeddings for liked products
    # Limit to most recent 100 likes to avoid query overhead
    recent_likes = liked_product_ids[:100]

    result = await pg.fetch_all("""
        SELECT e.image_embedding::float[] as embedding
        FROM embeddings.product_vectors e
        JOIN catalog.product_pricing pr ON e.product_id = pr.product_id
        WHERE e.product_id = ANY($1)
          AND e.image_embedding IS NOT NULL
          AND pr.is_active = true
    """, [recent_likes])

    if not result:
        logger.debug(f"No embeddings found for {len(recent_likes)} liked products, using global average")
        return await get_global_average_embedding(pg)

    # Average the embeddings
    embeddings = [np.array(row['embedding'], dtype=np.float32) for row in result]
    user_embedding = np.mean(embeddings, axis=0)

    logger.debug(f"Computed user embedding from {len(embeddings)} liked products")

    return user_embedding


async def get_user_embedding_weighted(
    pg: "AsyncPostgresClient",
    liked_product_ids: List[str],
    recency_weights: Optional[List[float]] = None,
) -> np.ndarray:
    """
    Compute user embedding with optional recency weighting.

    More recent likes can be weighted more heavily to capture
    the user's current preferences.

    Args:
        pg: Async PostgreSQL client
        liked_product_ids: List of product IDs (ordered by recency, most recent first)
        recency_weights: Optional weights for each like (same length as liked_product_ids)

    Returns:
        512-dim numpy array representing the user's embedding
    """
    if not liked_product_ids:
        return await get_global_average_embedding(pg)

    # Default: exponential decay weights (more recent = higher weight)
    if recency_weights is None:
        n = min(len(liked_product_ids), 100)
        decay_rate = 0.95
        recency_weights = [decay_rate ** i for i in range(n)]

    # Normalize weights
    weight_sum = sum(recency_weights[:len(liked_product_ids)])
    normalized_weights = [w / weight_sum for w in recency_weights]

    # Fetch embeddings
    recent_likes = liked_product_ids[:100]
    result = await pg.fetch_all("""
        SELECT e.product_id, e.image_embedding::float[] as embedding
        FROM embeddings.product_vectors e
        JOIN catalog.product_pricing pr ON e.product_id = pr.product_id
        WHERE e.product_id = ANY($1)
          AND e.image_embedding IS NOT NULL
          AND pr.is_active = true
    """, [recent_likes])

    if not result:
        return await get_global_average_embedding(pg)

    # Create mapping of product_id -> embedding
    emb_map = {row['product_id']: np.array(row['embedding'], dtype=np.float32) for row in result}

    # Weighted average (preserving order from liked_product_ids)
    weighted_sum = np.zeros(EMBEDDING_DIM, dtype=np.float32)
    total_weight = 0.0

    for i, pid in enumerate(recent_likes):
        if pid in emb_map:
            weight = normalized_weights[i] if i < len(normalized_weights) else normalized_weights[-1]
            weighted_sum += weight * emb_map[pid]
            total_weight += weight

    if total_weight > 0:
        user_embedding = weighted_sum / total_weight
    else:
        user_embedding = await get_global_average_embedding(pg)

    logger.debug(f"Computed weighted user embedding from {len(emb_map)} liked products")

    return user_embedding


def reset_global_cache() -> None:
    """Reset the global average embedding cache (useful for testing)."""
    global _GLOBAL_AVG_EMBEDDING
    _GLOBAL_AVG_EMBEDDING = None
