"""
Session-aware user embedding that blends long-term preferences with
real-time session signals.

Blending formula:
    final_embedding = alpha * long_term_embedding + (1 - alpha) * session_embedding

Where alpha decays as session engagement increases:
    alpha = base_alpha * decay_factor^(session_positive_count)
    alpha = max(0.3, alpha)  # Never below 30% long-term
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, List, Optional

import numpy as np

from .user_embedding import EMBEDDING_DIM, LEARNED_EMBEDDING_DIM, get_user_embedding

if TYPE_CHECKING:
    from ..connectors.postgres import AsyncPostgresClient

logger = logging.getLogger(__name__)


async def get_session_embedding(
    pg: "AsyncPostgresClient",
    session_product_ids: List[str],
    engagement_scores: Dict[str, float],
    use_learned: bool = False,
) -> Optional[np.ndarray]:
    """
    Compute session embedding from positively-engaged products.

    Uses engagement scores as weights for weighted average.
    Products with higher engagement contribute more to the session embedding.

    Args:
        pg: Async PostgreSQL client
        session_product_ids: Product IDs with positive engagement in this session
        engagement_scores: Dict mapping product_id -> engagement score
        use_learned: If True, use learned_embedding (128-dim); else image_embedding (512-dim)

    Returns:
        Weighted average embedding, or None if no valid products
    """
    if not session_product_ids:
        return None

    # Limit to most recent/highest-scored products for efficiency
    products_to_fetch = session_product_ids[:50]

    # Fetch embeddings for session products
    emb_col = "learned_embedding" if use_learned else "image_embedding"
    result = await pg.fetch_all(
        f"""
        SELECT e.product_id, e.{emb_col}::real[] as embedding
        FROM embeddings.product_vectors e
        JOIN catalog.product_pricing pr ON e.product_id = pr.product_id
        WHERE e.product_id = ANY($1)
          AND e.{emb_col} IS NOT NULL
          AND pr.is_active = true
        """,
        [products_to_fetch],
    )

    if not result:
        logger.debug("No embeddings found for session products")
        return None

    # Build weighted average using engagement scores
    embeddings = []
    weights = []

    for row in result:
        pid = row["product_id"]
        emb = row.get("embedding")

        if emb is None:
            continue

        emb_array = np.array(emb, dtype=np.float32)

        # Get engagement score, default to 0.5 if not found
        score = engagement_scores.get(pid, 0.5)

        # Ensure minimum weight to prevent zeros
        weight = max(0.1, score)

        embeddings.append(emb_array)
        weights.append(weight)

    if not embeddings:
        logger.debug("No valid embeddings after filtering")
        return None

    # Normalize weights
    weights = np.array(weights, dtype=np.float32)
    weights = weights / weights.sum()

    # Compute weighted average
    dim = LEARNED_EMBEDDING_DIM if use_learned else EMBEDDING_DIM
    session_embedding = np.zeros(dim, dtype=np.float32)
    for emb, weight in zip(embeddings, weights):
        session_embedding += weight * emb

    logger.debug(f"Computed session embedding from {len(embeddings)} products")
    return session_embedding


async def get_blended_user_embedding(
    pg: "AsyncPostgresClient",
    liked_product_ids: List[str],
    session_product_ids: List[str],
    engagement_scores: Dict[str, float],
    base_alpha: float = 0.7,
    decay_factor: float = 0.9,
    use_learned: bool = False,
) -> np.ndarray:
    """
    Blend long-term and session embeddings with adaptive weighting.

    The blending adapts based on session engagement:
    - Early session (few interactions): Rely more on long-term preferences
    - Active session (many positive interactions): Shift toward session preferences

    Formula:
        alpha = base_alpha * decay_factor^(session_positive_count)
        alpha = max(0.3, alpha)  # Never go below 30% long-term

        final = alpha * long_term + (1 - alpha) * session

    Args:
        pg: Async PostgreSQL client
        liked_product_ids: All-time liked product IDs (for long-term embedding)
        session_product_ids: Products with positive engagement this session
        engagement_scores: Dict mapping product_id -> engagement score
        base_alpha: Starting weight for long-term embedding (default 0.7 = 70%)
        decay_factor: How much to reduce long-term weight per session product
        use_learned: If True, use learned_embedding (128-dim); else image_embedding (512-dim)

    Returns:
        Blended user embedding, normalized
    """
    # Get long-term embedding (from all-time likes)
    long_term_emb = await get_user_embedding(pg, liked_product_ids, use_learned=use_learned)

    # Get session embedding (from this session's positive products)
    session_emb = await get_session_embedding(pg, session_product_ids, engagement_scores, use_learned=use_learned)

    # If no session data, return long-term only
    if session_emb is None:
        logger.debug("No session embedding available, using long-term only")
        return long_term_emb

    # Compute adaptive alpha
    # More session interactions = lower alpha = more session influence
    session_positive_count = len(session_product_ids)
    alpha = base_alpha * (decay_factor**session_positive_count)
    alpha = max(0.3, alpha)  # Never go below 30% long-term influence

    logger.debug(
        f"Blending embeddings: alpha={alpha:.2f} "
        f"(session has {session_positive_count} positive products)"
    )

    # Blend embeddings
    blended = alpha * long_term_emb + (1 - alpha) * session_emb

    # L2 normalize for consistent similarity computations
    norm = np.linalg.norm(blended)
    if norm > 0:
        blended = blended / norm

    return blended


# Cold-start stage definitions
STAGE_BRAND_NEW = "brand_new"  # No likes, no session data
STAGE_BROWSING = "browsing"  # No likes, but has session engagement
STAGE_FIRST_LIKE = "first_like"  # 1-3 likes
STAGE_EMERGING = "emerging"  # 4-10 likes
STAGE_ESTABLISHED = "established"  # 10+ likes


def determine_cold_start_stage(
    liked_count: int,
    session_positive_count: int,
) -> str:
    """
    Determine which cold-start stage the user is in.

    Stages determine retrieval strategy:
    - BRAND_NEW: Pure exploration (trending + fresh + random)
    - BROWSING: Use session signals for early personalization
    - FIRST_LIKE: Blended embedding with heavy session weight
    - EMERGING: Normal retrieval with blended embedding
    - ESTABLISHED: Fully personalized with session adaptation
    """
    if liked_count >= 10:
        return STAGE_ESTABLISHED
    elif liked_count >= 4:
        return STAGE_EMERGING
    elif liked_count >= 1:
        return STAGE_FIRST_LIKE
    elif session_positive_count >= 1:  # Was 3 — act on first signal
        return STAGE_BROWSING
    else:
        return STAGE_BRAND_NEW


# Epsilon values by cold-start stage (explore budget fraction)
_EPSILON_MAP = {
    STAGE_BRAND_NEW: 0.4,    # Heavy exploration for new users
    STAGE_BROWSING: 0.2,     # Moderate exploration (has some session signals)
    STAGE_FIRST_LIKE: 0.15,  # Slightly less explore
    STAGE_EMERGING: 0.10,    # Growing personalization
    STAGE_ESTABLISHED: 0.08, # Low explore, mostly exploit
}


def get_explore_epsilon(cold_start_stage: str, is_anonymous: bool = False) -> float:
    """
    Get explore budget fraction based on user's cold-start stage.

    Returns epsilon in [0, 1] — the fraction of the feed reserved for exploration.
    """
    if is_anonymous:
        return 0.4
    return _EPSILON_MAP.get(cold_start_stage, 0.15)
