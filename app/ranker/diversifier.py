from __future__ import annotations

from typing import Dict, List, Optional, Tuple
from collections import Counter, deque


def filter_seen_pairs(bucket_list: List[Tuple[str, float]], shown_set: set[str]) -> List[Tuple[str, float]]:
    return [(pid, score) for pid, score in bucket_list if pid not in shown_set]


def slice_buckets_by_ratio(
    personal: List[Tuple[str, float]],
    category: List[Tuple[str, float]],
    fresh: List[Tuple[str, float]],
    final_feed_size: int,
    ratios,
) -> Dict[str, List[str]]:
    num_personal = int(ratios.personal * final_feed_size)
    num_category = int(ratios.category * final_feed_size)
    num_fresh = int(ratios.fresh * final_feed_size)

    personal_slice = [pid for pid, _ in personal[:num_personal]]
    category_slice = [pid for pid, _ in category[:num_category]]
    fresh_slice = [pid for pid, _ in fresh[:num_fresh]]

    combined_len = len(personal_slice) + len(category_slice) + len(fresh_slice)

    # Backfill from personal if short
    if combined_len < final_feed_size:
        deficit = final_feed_size - combined_len
        backfill = [pid for pid, _ in personal[num_personal : num_personal + deficit]]
        personal_slice.extend(backfill)

    return {
        "personal": personal_slice,
        "category": category_slice,
        "fresh": fresh_slice,
    }


def interleave_buckets(
    slices: Dict[str, List[str]],
    final_feed_size: int,
    brand_map: Optional[Dict[str, Optional[str]]] = None
) -> List[str]:
    queues = {
        "personal": deque(slices.get("personal", [])),
        "category": deque(slices.get("category", [])),
        "fresh": deque(slices.get("fresh", [])),
    }

    pattern = ["personal", "personal", "personal", "category", "fresh"]

    final_feed: List[str] = []
    i = 0
    while len(final_feed) < final_feed_size and any(len(q) > 0 for q in queues.values()):
        bucket_name = pattern[i % len(pattern)]
        if queues[bucket_name]:
            final_feed.append(queues[bucket_name].popleft())
        else:
            # fallback: pop from any non-empty queue
            for name, q in queues.items():
                if q:
                    final_feed.append(q.popleft())
                    break
        i += 1

    # If still short, pad with whatever remains in personal then others
    if len(final_feed) < final_feed_size:
        for name in ["personal", "category", "fresh"]:
            while len(final_feed) < final_feed_size and queues[name]:
                final_feed.append(queues[name].popleft())
            if len(final_feed) >= final_feed_size:
                break

    # De-duplicate while preserving order
    seen = set()
    unique_final = []
    for pid in final_feed:
        if pid not in seen:
            seen.add(pid)
            unique_final.append(pid)
        if len(unique_final) == final_feed_size:
            break

    # Apply brand diversity if brand_map provided
    if brand_map:
        unique_final = enforce_brand_diversity(unique_final, brand_map)

    return unique_final


def enforce_brand_diversity(
    feed: List[str],
    brand_map: Dict[str, Optional[str]],
    max_consecutive: int = 2,
    window_size: int = 10,
    max_per_window: int = 3
) -> List[str]:
    """
    Reorder feed to prevent brand clustering.

    Args:
        feed: List of product IDs in current order
        brand_map: Mapping of product_id -> brand (None if unknown)
        max_consecutive: Max same-brand products allowed in a row
        window_size: Sliding window size for diversity check
        max_per_window: Max same-brand products allowed in any window

    Returns:
        Reordered feed with brand diversity enforced
    """
    if not feed or not brand_map:
        return feed

    result: List[str] = []
    remaining = deque(feed)
    deferred: List[str] = []

    while remaining:
        found = False
        # Try each remaining item to find one that satisfies diversity
        for _ in range(len(remaining)):
            pid = remaining.popleft()
            brand = brand_map.get(pid)

            # Products without brand always pass
            if brand is None:
                result.append(pid)
                found = True
                break

            # Check consecutive constraint
            consecutive_count = 0
            for i in range(len(result) - 1, -1, -1):
                prev_brand = brand_map.get(result[i])
                if prev_brand == brand:
                    consecutive_count += 1
                else:
                    break

            if consecutive_count >= max_consecutive:
                remaining.append(pid)  # Put back at end
                continue

            # Check window constraint
            window_start = max(0, len(result) - window_size + 1)
            window = result[window_start:]
            window_brands = [brand_map.get(p) for p in window]
            brand_count = sum(1 for b in window_brands if b == brand)

            if brand_count >= max_per_window:
                remaining.append(pid)  # Put back at end
                continue

            # Passes both constraints
            result.append(pid)
            found = True
            break

        if not found and remaining:
            # No item satisfies constraints, take first available to avoid infinite loop
            deferred.append(remaining.popleft())

    # Append deferred items at the end
    result.extend(deferred)

    return result
