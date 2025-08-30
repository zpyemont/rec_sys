from fastapi import FastAPI, Query
from typing import List, Dict, Any

from .settings import get_settings
from .schemas import FeedResponse, ProductItem

# Firestore for shown-set history
from .connectors.firestore import (
    get_firestore_client_safe,
    add_shown_items_fs,
    get_shown_set_fs,
)
from .connectors.postgres import PostgresClient
from .ranker.candidate_sources import (
    query_popular_ids,
    query_recent_ids,
    query_top_by_category,
    fetch_freshness_metrics,
    fetch_features_for_ids,
    join_product_metadata,
)
from .ranker.diversifier import (
    slice_buckets_by_ratio,
    interleave_buckets,
    filter_seen_pairs,
)
from .ranker.model import score_with_model_or_fallback

app = FastAPI(title="Ranker Service", version="0.1.0")
settings = get_settings()


@app.get("/healthz")
def healthz() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/get_diverse_feed", response_model=FeedResponse)
def get_diverse_feed(user_id: str = Query(...), device: str | None = Query(None), n: int | None = Query(None)) -> FeedResponse:
    final_feed_size = n or settings.feed_default_size

    fs_client = get_firestore_client_safe(settings)
    pg_client = PostgresClient.from_settings(settings)

    shown_set = get_shown_set_fs(fs_client, user_id)

    # Step 2: Assemble candidate pools (stubbed)
    popular_ids = query_popular_ids(pg_client, limit=5000)
    recent_ids = query_recent_ids(pg_client, hours=24, limit=1000)
    candidates_raw = list(dict.fromkeys(popular_ids + recent_ids))

    candidates_unseen = [pid for pid in candidates_raw if pid not in shown_set]

    # Batch features for scoring (stub)
    features = fetch_features_for_ids(candidates_unseen)

    # Mock model scores with fallback
    personal_scores = score_with_model_or_fallback(features, fallback_scores=None)
    personalized_sorted = sorted(personal_scores.items(), key=lambda x: x[1], reverse=True)

    # Category/style diversification bucket (stubbed)
    top_categories: List[str] = []
    cat_bucket_ids: List[str] = []
    for cat in top_categories:
        cat_bucket_ids.extend(query_top_by_category(pg_client, cat, limit=200))
    cat_bucket_unseen = [pid for pid in cat_bucket_ids if pid not in shown_set]
    cat_features = fetch_features_for_ids(cat_bucket_unseen)
    cat_scores = score_with_model_or_fallback(cat_features, fallback_scores=None)
    cat_div_sorted = sorted(cat_scores.items(), key=lambda x: x[1], reverse=True)

    # Fresh items exploration bucket
    recent_pool = query_recent_ids(pg_client, hours=24, limit=1000)
    recent_unseen = [pid for pid in recent_pool if pid not in shown_set]
    freshness_metrics = fetch_freshness_metrics(recent_unseen)
    fresh_features = fetch_features_for_ids(list(freshness_metrics.keys()))
    fresh_model_scores = score_with_model_or_fallback(fresh_features, fallback_scores=freshness_metrics)
    fresh_div_sorted = sorted(fresh_model_scores.items(), key=lambda x: x[1], reverse=True)

    # Step 3: ensure seen are filtered
    personalized_sorted = filter_seen_pairs(personalized_sorted, shown_set)
    cat_div_sorted = filter_seen_pairs(cat_div_sorted, shown_set)
    fresh_div_sorted = filter_seen_pairs(fresh_div_sorted, shown_set)

    # Step 5: blend/merge by ratios
    slices = slice_buckets_by_ratio(
        personalized_sorted,
        cat_div_sorted,
        fresh_div_sorted,
        final_feed_size=final_feed_size,
        ratios=settings.bucket_ratios,
    )

    # Step 7.3: interleave
    final_ids = interleave_buckets(slices, final_feed_size)

    # Step 8: record shown
    add_shown_items_fs(fs_client, user_id, final_ids)

    # Hydrate metadata from Postgres when available
    hydrated: List[Dict[str, Any]] = join_product_metadata(pg_client, final_ids)

    items = [
        ProductItem(
            prod_id=str(item.get("prod_id")),
            image_url=item.get("image_url"),
            price=float(item.get("price")) if item.get("price") is not None else None,
            title=item.get("title"),
        )
        for item in hydrated
    ]

    return FeedResponse(feed=items)
