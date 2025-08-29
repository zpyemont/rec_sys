from __future__ import annotations

from typing import Dict, List, Tuple
from collections import deque


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


def interleave_buckets(slices: Dict[str, List[str]], final_feed_size: int) -> List[str]:
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

    return unique_final
