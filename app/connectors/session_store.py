"""
In-memory session store for tracking real-time user behavior within a browsing session.

This module manages:
1. Session lifecycle (creation, expiration via TTL)
2. Position history for revisit detection
3. Per-product engagement aggregation
4. Engagement score computation with correct signal weights

For MVP: Uses in-memory TTLCache. Can migrate to Redis later for horizontal scaling.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from cachetools import TTLCache

logger = logging.getLogger(__name__)

# Session configuration
SESSION_TTL_SECONDS = 1800  # 30 minutes
SESSION_MAX_SIZE = 10000  # Max concurrent sessions

# Global in-memory cache with TTL
_session_cache: TTLCache = TTLCache(maxsize=SESSION_MAX_SIZE, ttl=SESSION_TTL_SECONDS)
_lock = threading.Lock()


@dataclass
class ProductEngagement:
    """Engagement data for a single product within a session."""

    product_id: str
    total_dwell_time: float = 0.0
    view_count: int = 0
    max_images_viewed: int = 0
    last_position: int = 0
    actions: List[str] = field(default_factory=list)
    engagement_score: float = 0.0
    is_revisit: bool = False


@dataclass
class SessionData:
    """Complete session state for a user."""

    user_id: str
    session_id: str
    max_position: int = 0
    products: Dict[str, ProductEngagement] = field(default_factory=dict)
    # Track position history: list of (product_id, position) in order of interaction
    position_history: List[Tuple[str, int]] = field(default_factory=list)


def _session_key(user_id: str, session_id: str) -> str:
    """Generate cache key for a session."""
    return f"{user_id}:{session_id}"


def _compute_engagement_score(
    engagement: ProductEngagement,
    is_revisit: bool,
) -> float:
    """
    Compute engagement score based on user's signal interpretation.

    Signal weights (per user's clarification):
    - swipe_up: -0.1 (just moving on, NOT positive)
    - swipe_down: -0.3 (active dismissal)
    - like: +1.0 (explicit positive)
    - collection_add: +1.0 (explicit positive)
    - shop_now: +1.2 (purchase intent, strongest positive)
    - revisit (landing): +0.5 (deliberate return to product)
    - high dwell_time: +0.1 to +0.6 (diminishing returns after 3s)
    - fast swipe (<1s + swipe): -0.2 (quick rejection)
    - multiple images: +0.1 per additional image (up to +0.3)
    """
    score = 0.0

    # Action-based scores
    action_weights = {
        "like": 1.0,
        "unlike": -0.5,  # Unliking is negative
        "collection_add": 1.0,
        "shop_now": 1.2,
        "swipe_up": -0.1,  # Just moving on
        "swipe_down": -0.3,  # Active dismissal
    }

    for action in engagement.actions:
        score += action_weights.get(action, 0.0)

    # Revisit bonus - only the "landing" product gets this
    if is_revisit and engagement.view_count > 1:
        score += 0.5

    # Dwell time contribution (diminishing returns)
    # First 3 seconds: linear contribution
    # After 3 seconds: diminishing returns, max +0.6
    if engagement.total_dwell_time >= 3.0:
        dwell_bonus = min(0.6, 0.2 + 0.1 * (engagement.total_dwell_time - 3.0) / 3.0)
        score += dwell_bonus
    elif engagement.total_dwell_time >= 1.0:
        # 1-3 seconds: small positive
        score += 0.1

    # Fast swipe penalty (quick rejection)
    # Only apply if last action was a swipe and dwell was very short
    if engagement.actions:
        last_action = engagement.actions[-1]
        if last_action in ("swipe_up", "swipe_down") and engagement.total_dwell_time < 1.0:
            score -= 0.2

    # Multiple images viewed bonus
    if engagement.max_images_viewed > 1:
        image_bonus = 0.1 * min(engagement.max_images_viewed - 1, 3)
        score += image_bonus

    return score


def get_or_create_session(user_id: str, session_id: str) -> SessionData:
    """
    Get existing session or create new one.

    Sessions auto-expire after 30 minutes of inactivity via TTLCache.
    """
    key = _session_key(user_id, session_id)

    with _lock:
        if key in _session_cache:
            return _session_cache[key]

        # Create new session
        session = SessionData(user_id=user_id, session_id=session_id)
        _session_cache[key] = session
        logger.debug(f"Created new session: {key}")
        return session


def record_view(
    user_id: str,
    session_id: str,
    product_id: str,
    position: int,
    dwell_time: float,
    images_viewed: int,
    action: str,
) -> ProductEngagement:
    """
    Record a product view/interaction within the session.

    Detects revisits: if current position < session's max_position,
    user has scrolled back. Only the "landing" product (where they stop)
    gets the revisit bonus - intermediate products passed while scrolling
    don't count.

    Returns:
        ProductEngagement with updated scores and is_revisit flag
    """
    session = get_or_create_session(user_id, session_id)

    with _lock:
        # Detect revisit: user scrolled back to a lower position
        # This means they deliberately returned to this product
        is_revisit = position < session.max_position

        # Get or create product engagement
        if product_id not in session.products:
            session.products[product_id] = ProductEngagement(product_id=product_id)

        engagement = session.products[product_id]

        # Update engagement data
        engagement.total_dwell_time += dwell_time
        engagement.view_count += 1
        engagement.max_images_viewed = max(engagement.max_images_viewed, images_viewed)
        engagement.last_position = position
        engagement.actions.append(action)
        engagement.is_revisit = is_revisit or engagement.is_revisit  # Sticky flag

        # Compute engagement score
        engagement.engagement_score = _compute_engagement_score(engagement, is_revisit)

        # Update session state
        session.max_position = max(session.max_position, position)
        session.position_history.append((product_id, position))

        # Re-insert to refresh TTL
        key = _session_key(user_id, session_id)
        _session_cache[key] = session

        logger.debug(
            f"Recorded view: user={user_id}, session={session_id}, "
            f"product={product_id}, action={action}, "
            f"is_revisit={is_revisit}, score={engagement.engagement_score:.2f}"
        )

        return engagement


def get_session_positive_products(
    user_id: str,
    session_id: str,
    min_score: float = 0.3,
    limit: int = 30,
) -> List[str]:
    """
    Get products with positive engagement in current session.

    Returns product IDs ordered by engagement score (descending).
    """
    key = _session_key(user_id, session_id)

    with _lock:
        if key not in _session_cache:
            return []

        session = _session_cache[key]

        # Filter to positive products and sort by score
        positive = [
            (pid, eng.engagement_score)
            for pid, eng in session.products.items()
            if eng.engagement_score >= min_score
        ]

        positive.sort(key=lambda x: x[1], reverse=True)

        return [pid for pid, _ in positive[:limit]]


def get_session_engagement_map(
    user_id: str,
    session_id: str,
) -> Dict[str, float]:
    """
    Get all product engagement scores for the session.

    Returns dict mapping product_id -> engagement_score.
    """
    key = _session_key(user_id, session_id)

    with _lock:
        if key not in _session_cache:
            return {}

        session = _session_cache[key]
        return {pid: eng.engagement_score for pid, eng in session.products.items()}


def get_session_stats(user_id: str, session_id: str) -> Optional[Dict]:
    """Get session statistics for debugging/logging."""
    key = _session_key(user_id, session_id)

    with _lock:
        if key not in _session_cache:
            return None

        session = _session_cache[key]
        positive_count = sum(
            1 for eng in session.products.values() if eng.engagement_score > 0
        )

        return {
            "session_id": session_id,
            "total_products_viewed": len(session.products),
            "positive_products": positive_count,
            "max_position": session.max_position,
            "interactions": len(session.position_history),
        }
