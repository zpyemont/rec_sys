from fastapi import FastAPI, Query, HTTPException
from typing import List, Dict, Any
import time
import logging

from .settings import get_settings
from .schemas import (
    FeedResponse,
    ProductItem,
    LikeRequest,
    LikeResponse,
    CollectionItem,
    CollectionsResponse,
    TrackRequest,
    TrackResponse,
)

# Firestore for shown-set history
from .connectors.firestore import (
    get_firestore_client_safe,
    add_shown_items_fs,
    get_shown_set_fs,
)
from .connectors.postgres import PostgresClient
from .connectors.kafka import get_kafka_producer
from .connectors.tfs_client import get_monolith_client
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
from .utils import generate_request_id

logger = logging.getLogger(__name__)

app = FastAPI(title="Ranker Service", version="0.1.0")
settings = get_settings()


@app.get("/healthz")
def healthz() -> Dict[str, str]:
    return {"status": "ok"}


def record_user_like_firestore(fs_client, user_id: str, product_id: str, liked: bool):
    """
    Record or remove user like in Firestore.
    Path: /users/{user_id}/likes/product_{product_id}
    """
    if not fs_client:
        return

    from google.cloud import firestore as fs

    doc_ref = fs_client.client.collection("users").document(user_id).collection("likes").document(f"product_{product_id}")

    if liked:
        # Add like
        doc_ref.set({
            "type": "product",
            "created_at": fs.SERVER_TIMESTAMP
        })
    else:
        # Remove like
        doc_ref.delete()


@app.post("/like", response_model=LikeResponse)
def like_product(request: LikeRequest) -> LikeResponse:
    """
    Like a product: increments like_count in PostgreSQL and records in Firestore.
    Anonymous users are not allowed to like products.
    """
    # Block anonymous users from liking
    if request.user_id == "anonymous":
        return LikeResponse(
            success=False,
            like_count=0,
            message="You must be logged in to like products"
        )

    try:
        pg_client = PostgresClient.from_settings(settings)

        # Increment like count in PostgreSQL
        new_like_count = pg_client.increment_like_count(request.product_id)

        # Record like in Firestore
        fs_client = get_firestore_client_safe(settings)
        record_user_like_firestore(fs_client, request.user_id, request.product_id, liked=True)

        return LikeResponse(
            success=True,
            like_count=new_like_count,
            message="Product liked successfully"
        )
    except Exception as e:
        return LikeResponse(
            success=False,
            like_count=0,
            message=f"Error liking product: {str(e)}"
        )


@app.post("/unlike", response_model=LikeResponse)
def unlike_product(request: LikeRequest) -> LikeResponse:
    """
    Unlike a product: decrements like_count in PostgreSQL and removes from Firestore.
    Anonymous users are not allowed to unlike products.
    """
    # Block anonymous users from unliking
    if request.user_id == "anonymous":
        return LikeResponse(
            success=False,
            like_count=0,
            message="You must be logged in to unlike products"
        )

    try:
        pg_client = PostgresClient.from_settings(settings)

        # Decrement like count in PostgreSQL
        new_like_count = pg_client.decrement_like_count(request.product_id)

        # Remove like from Firestore
        fs_client = get_firestore_client_safe(settings)
        record_user_like_firestore(fs_client, request.user_id, request.product_id, liked=False)

        return LikeResponse(
            success=True,
            like_count=new_like_count,
            message="Product unliked successfully"
        )
    except Exception as e:
        return LikeResponse(
            success=False,
            like_count=0,
            message=f"Error unliking product: {str(e)}"
        )


@app.get("/liked-products", response_model=FeedResponse)
def get_liked_products(user_id: str = Query(...)) -> FeedResponse:
    """
    Get all liked products for a user.
    Fetches likes from Firestore and joins with PostgreSQL product data.
    """
    # Block anonymous users
    if user_id == "anonymous":
        return FeedResponse(feed=[])

    try:
        fs_client = get_firestore_client_safe(settings)
        pg_client = PostgresClient.from_settings(settings)

        if not fs_client:
            raise HTTPException(status_code=500, detail="Firestore not available")

        # Fetch user's likes from Firestore
        likes_ref = fs_client.client.collection("users").document(user_id).collection("likes")
        likes_docs = likes_ref.stream()

        # Extract product IDs from likes (filter for product likes only)
        product_ids = []
        for doc in likes_docs:
            # DocumentSnapshot has .to_dict() method
            data = doc.to_dict() if hasattr(doc, 'to_dict') else {}
            if data and data.get("type") == "product" and doc.id.startswith("product_"):
                product_id = doc.id.replace("product_", "")
                product_ids.append(product_id)

        if not product_ids:
            return FeedResponse(feed=[])

        # Join with PostgreSQL to get full product metadata
        product_metadata: Dict[str, dict] = join_product_metadata(pg_client, product_ids)

        # Build response with full product details
        items = [
            ProductItem(
                id=pid,
                title=meta.get("title"),
                price=meta.get("price"),
                images=meta.get("images", []),
                category=meta.get("category"),
                like_count=meta.get("like_count", 0),
                description=meta.get("description"),
                url=meta.get("url"),
                brand=meta.get("brand"),
                created_at=meta.get("created_at"),
                currency=meta.get("currency"),
                availability=meta.get("availability")
            )
            for pid in product_ids
            if (meta := product_metadata.get(pid))
        ]

        return FeedResponse(feed=items)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching liked products: {str(e)}")


@app.get("/collections", response_model=CollectionsResponse)
def get_collections(user_id: str = Query(...)) -> CollectionsResponse:
    """
    Get all collections for a user with their products.
    Fetches collections from Firestore and joins products with PostgreSQL.
    """
    # Block anonymous users
    if user_id == "anonymous":
        return CollectionsResponse(collections=[])

    try:
        fs_client = get_firestore_client_safe(settings)
        pg_client = PostgresClient.from_settings(settings)

        if not fs_client:
            raise HTTPException(status_code=500, detail="Firestore not available")

        # Fetch user's collections from Firestore
        collections_ref = fs_client.client.collection("users").document(user_id).collection("collections")
        collections_docs = collections_ref.stream()

        collections_list = []

        for coll_doc in collections_docs:
            # DocumentSnapshot has .to_dict() method
            coll_data = coll_doc.to_dict() if hasattr(coll_doc, 'to_dict') else {}
            if not coll_data:
                continue

            collection_id = coll_doc.id

            # Fetch products in this collection (items subcollection)
            products_ref = collections_ref.document(collection_id).collection("items")
            products_docs = products_ref.stream()

            # Extract product IDs (document IDs are the product IDs)
            product_ids = []
            for pdoc in products_docs:
                # In the frontend, the document ID IS the product ID
                product_ids.append(pdoc.id)

            # Join with PostgreSQL to get full product metadata
            product_metadata: Dict[str, dict] = join_product_metadata(pg_client, product_ids) if product_ids else {}

            # Build product items
            products = [
                ProductItem(
                    id=pid,
                    title=meta.get("title"),
                    price=meta.get("price"),
                    images=meta.get("images", []),
                    category=meta.get("category"),
                    like_count=meta.get("like_count", 0),
                    description=meta.get("description"),
                    url=meta.get("url"),
                    brand=meta.get("brand"),
                    created_at=meta.get("created_at"),
                    currency=meta.get("currency"),
                    availability=meta.get("availability")
                )
                for pid in product_ids
                if (meta := product_metadata.get(pid))
            ]

            # Convert Firestore timestamps to ISO strings
            created_at = coll_data.get("created_at")
            updated_at = coll_data.get("updated_at")

            # Handle Firestore timestamp conversion
            if created_at:
                # Firestore timestamps might be datetime objects or need conversion
                if hasattr(created_at, 'isoformat'):
                    created_at_str = created_at.isoformat()
                elif hasattr(created_at, 'ToDatetime'):
                    created_at_str = created_at.ToDatetime().isoformat()
                else:
                    created_at_str = str(created_at)
            else:
                created_at_str = ""

            if updated_at:
                if hasattr(updated_at, 'isoformat'):
                    updated_at_str = updated_at.isoformat()
                elif hasattr(updated_at, 'ToDatetime'):
                    updated_at_str = updated_at.ToDatetime().isoformat()
                else:
                    updated_at_str = str(updated_at)
            else:
                updated_at_str = ""

            collection_item = CollectionItem(
                id=collection_id,
                name=coll_data.get("name", "Untitled"),
                created_at=created_at_str,
                updated_at=updated_at_str,
                product_count=coll_data.get("product_count", 0),
                products=products
            )
            collections_list.append(collection_item)

        return CollectionsResponse(collections=collections_list)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching collections: {str(e)}")


@app.get("/get_diverse_feed", response_model=FeedResponse)
def get_diverse_feed(user_id: str = Query(...), device: str | None = Query(None), n: int | None = Query(None)) -> FeedResponse:
    final_feed_size = n or settings.feed_default_size

    # Generate unique request ID for this recommendation request
    request_id = generate_request_id(worker_id=settings.worker_id)
    event_time = int(time.time() * 1000)

    fs_client = get_firestore_client_safe(settings)
    pg_client = PostgresClient.from_settings(settings)

    # Skip Firestore tracking for anonymous users
    is_anonymous = user_id == "anonymous"
    shown_set = set() if is_anonymous else get_shown_set_fs(fs_client, user_id)

    # Step 2: Assemble candidate pools
    popular_ids = query_popular_ids(pg_client, limit=5000)
    recent_ids = query_recent_ids(pg_client, hours=24, limit=1000)
    candidates_raw = list(dict.fromkeys(popular_ids + recent_ids))

    candidates_unseen = [pid for pid in candidates_raw if pid not in shown_set]

    # Step 3: Score candidates with Monolith (if enabled)
    if settings.monolith_enabled and not is_anonymous and candidates_unseen:
        try:
            # Limit candidates to top 500 for efficiency
            candidates_to_score = candidates_unseen[:500]

            # Call Monolith for predictions
            monolith_client = get_monolith_client(settings)
            user_emb, product_embs, scores = monolith_client.predict(
                user_id=user_id,
                product_ids=candidates_to_score
            )

            # Publish FeatureEvent to Kafka (if enabled)
            if settings.kafka_enabled:
                try:
                    kafka_producer = get_kafka_producer(settings)

                    # Build feature data
                    feature_data = {
                        "user_id": user_id,
                        "user_embedding": user_emb.tolist(),
                        "context": {
                            "session_position": 1,  # TODO: Track from session
                            "hour_of_day": time.localtime().tm_hour,
                            "day_of_week": time.localtime().tm_wday,
                            "device": device or "unknown"
                        },
                        "candidates": [
                            {
                                "product_id": pid,
                                "product_embedding": product_embs[pid].tolist(),
                                "monolith_score": scores[pid],
                                "position": i
                            }
                            for i, pid in enumerate(list(scores.keys())[:20])  # Top 20
                        ]
                    }

                    kafka_producer.publish_feature_event(request_id, event_time, feature_data)
                    logger.info(f"Published FeatureEvent for request_id={request_id}")
                except Exception as e:
                    logger.error(f"Failed to publish FeatureEvent: {e}")

            # Use Monolith scores
            personal_scores = scores
            logger.info(f"Monolith predictions successful: {len(personal_scores)} products scored")

        except Exception as e:
            logger.error(f"Monolith prediction failed: {e}, falling back to stub scoring")
            # Fallback to existing logic
            features = fetch_features_for_ids(candidates_unseen)
            personal_scores = score_with_model_or_fallback(features, fallback_scores=None)
    else:
        # Use existing fallback scoring (Monolith disabled or anonymous user)
        features = fetch_features_for_ids(candidates_unseen)
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

    # Step 8: record shown (skip for anonymous users)
    if not is_anonymous:
        add_shown_items_fs(fs_client, user_id, final_ids)

    # Hydrate metadata from Postgres when available
    product_metadata: Dict[str, dict] = join_product_metadata(pg_client, final_ids)

    # Build feed response with full product details
    items = [
        ProductItem(
            id=pid,
            title=meta.get("title"),
            price=meta.get("price"),
            images=meta.get("images", []),
            category=meta.get("category"),
            like_count=meta.get("like_count", 0),
            description=meta.get("description"),
            url=meta.get("url"),
            brand=meta.get("brand"),
            created_at=meta.get("created_at"),
            currency=meta.get("currency"),
            availability=meta.get("availability")
        )
        for pid in final_ids
        if (meta := product_metadata.get(pid))
    ]

    return FeedResponse(feed=items)


@app.post("/track", response_model=TrackResponse)
def track_interaction(request: TrackRequest) -> TrackResponse:
    """
    Track user interaction and publish ActionEvent to Kafka

    This endpoint receives user interaction events (swipes, likes, etc.)
    and publishes them to Kafka for joining with FeatureEvents
    """
    # Ignore anonymous users
    if request.user_id == "anonymous":
        return TrackResponse(
            status="ignored",
            request_id=request.request_id,
            message="Anonymous users not tracked"
        )

    # Only publish if Kafka is enabled
    if not settings.kafka_enabled:
        return TrackResponse(
            status="skipped",
            request_id=request.request_id,
            message="Kafka publishing disabled"
        )

    event_time = int(time.time() * 1000)

    # Determine label (1.0 for positive actions, 0.0 for negative)
    positive_actions = {"swipe_up", "like", "collection_add", "shop_now"}
    label = 1.0 if request.action in positive_actions else 0.0

    action_data = {
        "user_id": request.user_id,
        "product_id": request.product_id,
        "action": request.action,
        "dwell_time": request.dwell_time,
        "images_viewed": request.images_viewed,
        "position": request.position,
        "label": label
    }

    try:
        kafka_producer = get_kafka_producer(settings)
        kafka_producer.publish_action_event(request.request_id, event_time, action_data)

        logger.info(
            f"Published ActionEvent: request_id={request.request_id}, "
            f"user={request.user_id}, product={request.product_id}, action={request.action}"
        )

        return TrackResponse(
            status="tracked",
            request_id=request.request_id,
            message="Interaction tracked successfully"
        )
    except Exception as e:
        logger.error(f"Failed to publish ActionEvent: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to track interaction: {str(e)}"
        )
