"""
User embedding computation for personalized candidate retrieval.

Computes user embeddings by averaging product embeddings from liked items.
Supports both CLIP (512-dim) and two-tower learned (128-dim) embeddings.
For cold-start users (no likes), falls back to a global average embedding.
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

# Embedding dimensions
EMBEDDING_DIM = 512       # CLIP image embeddings
LEARNED_EMBEDDING_DIM = 128  # Two-tower learned embeddings


async def get_global_average_embedding(
    pg: "AsyncPostgresClient",
    use_learned: bool = False,
) -> np.ndarray:
    """
    Get the global average embedding across all products.

    This is used as a fallback for cold-start users who have no likes.
    The result is cached for subsequent calls.

    Args:
        pg: Async PostgreSQL client
        use_learned: If True, use learned_embedding (128-dim); else image_embedding (512-dim)

    Returns:
        numpy array representing the average product embedding
    """
    global _GLOBAL_AVG_EMBEDDING

    if _GLOBAL_AVG_EMBEDDING is not None:
        return _GLOBAL_AVG_EMBEDDING

    emb_col = "learned_embedding" if use_learned else "image_embedding"
    dim = LEARNED_EMBEDDING_DIM if use_learned else EMBEDDING_DIM

    logger.info(f"Computing global average embedding (one-time, col={emb_col})")

    result = await pg.fetch_all(f"""
        SELECT AVG(e.{emb_col})::real[] as avg_emb
        FROM embeddings.product_vectors e
        JOIN catalog.product_pricing pr ON e.product_id = pr.product_id
        WHERE e.{emb_col} IS NOT NULL
          AND pr.is_active = true
    """)

    if result and result[0]['avg_emb']:
        _GLOBAL_AVG_EMBEDDING = np.array(result[0]['avg_emb'], dtype=np.float32)
        logger.info(f"Global average embedding computed: shape {_GLOBAL_AVG_EMBEDDING.shape}")
    else:
        logger.warning("No embeddings found, using zero vector as fallback")
        _GLOBAL_AVG_EMBEDDING = np.zeros(dim, dtype=np.float32)

    return _GLOBAL_AVG_EMBEDDING


async def get_user_embedding(
    pg: "AsyncPostgresClient",
    liked_product_ids: List[str],
    use_learned: bool = False,
) -> np.ndarray:
    """
    Compute user embedding as the average of liked product embeddings.

    For users with no likes (cold-start), returns the global average embedding.

    Args:
        pg: Async PostgreSQL client
        liked_product_ids: List of product IDs the user has liked
        use_learned: If True, use learned_embedding (128-dim); else image_embedding (512-dim)

    Returns:
        numpy array representing the user's embedding
    """
    if not liked_product_ids:
        logger.debug("Cold-start user, using global average embedding")
        return await get_global_average_embedding(pg, use_learned=use_learned)

    recent_likes = liked_product_ids[:100]
    emb_col = "learned_embedding" if use_learned else "image_embedding"

    result = await pg.fetch_all(f"""
        SELECT e.{emb_col}::real[] as embedding
        FROM embeddings.product_vectors e
        JOIN catalog.product_pricing pr ON e.product_id = pr.product_id
        WHERE e.product_id = ANY($1)
          AND e.{emb_col} IS NOT NULL
          AND pr.is_active = true
    """, [recent_likes])

    if not result:
        logger.debug(f"No embeddings found for {len(recent_likes)} liked products, using global average")
        return await get_global_average_embedding(pg, use_learned=use_learned)

    embeddings = [np.array(row['embedding'], dtype=np.float32) for row in result]
    avg = np.mean(embeddings, axis=0)

    # L2 normalize for cosine similarity (important for learned embeddings)
    norm = np.linalg.norm(avg)
    if norm > 0:
        avg = avg / norm

    logger.debug(f"Computed user embedding from {len(embeddings)} liked products (col={emb_col})")
    return avg


async def get_user_embedding_weighted(
    pg: "AsyncPostgresClient",
    liked_product_ids: List[str],
    recency_weights: Optional[List[float]] = None,
    use_learned: bool = False,
) -> np.ndarray:
    """
    Compute user embedding with optional recency weighting.

    More recent likes can be weighted more heavily to capture
    the user's current preferences.

    Args:
        pg: Async PostgreSQL client
        liked_product_ids: List of product IDs (ordered by recency, most recent first)
        recency_weights: Optional weights for each like (same length as liked_product_ids)
        use_learned: If True, use learned_embedding (128-dim); else image_embedding (512-dim)

    Returns:
        numpy array representing the user's embedding
    """
    if not liked_product_ids:
        return await get_global_average_embedding(pg, use_learned=use_learned)

    if recency_weights is None:
        n = min(len(liked_product_ids), 100)
        decay_rate = 0.95
        recency_weights = [decay_rate ** i for i in range(n)]

    weight_sum = sum(recency_weights[:len(liked_product_ids)])
    normalized_weights = [w / weight_sum for w in recency_weights]

    recent_likes = liked_product_ids[:100]
    emb_col = "learned_embedding" if use_learned else "image_embedding"
    dim = LEARNED_EMBEDDING_DIM if use_learned else EMBEDDING_DIM

    result = await pg.fetch_all(f"""
        SELECT e.product_id, e.{emb_col}::real[] as embedding
        FROM embeddings.product_vectors e
        JOIN catalog.product_pricing pr ON e.product_id = pr.product_id
        WHERE e.product_id = ANY($1)
          AND e.{emb_col} IS NOT NULL
          AND pr.is_active = true
    """, [recent_likes])

    if not result:
        return await get_global_average_embedding(pg, use_learned=use_learned)

    emb_map = {row['product_id']: np.array(row['embedding'], dtype=np.float32) for row in result}

    weighted_sum = np.zeros(dim, dtype=np.float32)
    total_weight = 0.0

    for i, pid in enumerate(recent_likes):
        if pid in emb_map:
            weight = normalized_weights[i] if i < len(normalized_weights) else normalized_weights[-1]
            weighted_sum += weight * emb_map[pid]
            total_weight += weight

    if total_weight > 0:
        user_embedding = weighted_sum / total_weight
    else:
        user_embedding = await get_global_average_embedding(pg, use_learned=use_learned)

    logger.debug(f"Computed weighted user embedding from {len(emb_map)} liked products")

    return user_embedding


def reset_global_cache() -> None:
    """Reset the global average embedding cache (useful for testing)."""
    global _GLOBAL_AVG_EMBEDDING
    _GLOBAL_AVG_EMBEDDING = None
