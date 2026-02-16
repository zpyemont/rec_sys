from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict
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
    SearchResponse,
)

# Firestore for shown-set history
from .connectors.firestore import (
    get_firestore_client_safe,
    add_shown_items_fs,
    get_shown_set_fs,
)
from .connectors.postgres import PostgresClient, AsyncPostgresClient
from .connectors.redis_client import get_async_redis_client
from .connectors.kafka import get_kafka_producer
from .connectors.tfs_client import get_monolith_client

# Old heuristic candidate sources (legacy)
from .ranker.candidate_sources import (
    query_recent_ids,
    query_top_by_category,
    fetch_freshness_metrics,
    fetch_features_for_ids,
    join_product_metadata,
    get_dynamic_personalized_pool,
    get_user_liked_product_ids,
)
from .ranker.diversifier import (
    slice_buckets_by_ratio,
    interleave_buckets,
    filter_seen_pairs,
    enforce_brand_diversity,
)
from .ranker.model import score_with_model_or_fallback

# New embedding-based retrieval
from .ranker.retrieval import get_candidates_parallel, get_candidates_for_anonymous

# Session-aware features
from .connectors.session_store import (
    record_view as session_record_view,
    get_session_positive_products,
    get_session_engagement_map,
)
from .ranker.session_embedding import (
    get_blended_user_embedding,
    get_session_embedding,
    determine_cold_start_stage,
    STAGE_BRAND_NEW,
    STAGE_BROWSING,
)

# Hybrid search
from .ranker.search import HybridSearcher, EmbeddingService

from .utils import generate_request_id

logger = logging.getLogger(__name__)

app = FastAPI(title="Ranker Service", version="0.1.0")
settings = get_settings()

# CORS middleware for Capacitor iOS app and web
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "capacitor://localhost",  # iOS Capacitor
        "http://localhost",       # Android Capacitor
        "http://localhost:3000",  # Next.js dev
        "https://localhost",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
                compare_at_price=meta.get("compare_at_price"),
                images=meta.get("images", []),
                image_has_text=meta.get("image_has_text"),
                category=meta.get("category"),
                subcategory=meta.get("subcategory"),
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
                    compare_at_price=meta.get("compare_at_price"),
                    images=meta.get("images", []),
                    image_has_text=meta.get("image_has_text"),
                    category=meta.get("category"),
                    subcategory=meta.get("subcategory"),
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
async def get_diverse_feed(
    user_id: str = Query(...),
    device: str | None = Query(None),
    n: int | None = Query(None),
    exclude_ids: str | None = Query(None),
    session_id: str | None = Query(None),  # Client-generated session ID
) -> FeedResponse:
    """
    Get diverse feed of product recommendations.

    Uses embedding-based retrieval (pgvector) when enabled, otherwise
    falls back to heuristic candidate generation.

    Session-aware features (when session_id provided):
    - Blends long-term preferences (likes) with session signals
    - Adapts recommendations based on in-session engagement
    - Cold-start strategy based on available signals
    """
    final_feed_size = n or settings.feed_default_size

    # Generate unique request ID for this recommendation request
    request_id = generate_request_id(worker_id=settings.worker_id)
    event_time = int(time.time() * 1000)

    fs_client = get_firestore_client_safe(settings)
    is_anonymous = user_id == "anonymous"

    # Parse client-side exclude_ids (for anonymous users or supplemental filtering)
    client_shown_set = set()
    if exclude_ids:
        try:
            client_shown_set = set(exclude_ids.split(','))
        except Exception as e:
            logger.warning(f"Failed to parse exclude_ids: {e}")

    # Get server-side shown set (empty for anonymous users)
    server_shown_set = set() if is_anonymous else get_shown_set_fs(fs_client, user_id, ttl_days=settings.shown_set_ttl_days)

    # Merge client and server shown sets
    shown_set = client_shown_set | server_shown_set
    shown_count = len(shown_set)

    # Use embedding-based retrieval if enabled
    if settings.embedding_retrieval_enabled:
        return await _get_diverse_feed_embedding(
            user_id=user_id,
            device=device,
            final_feed_size=final_feed_size,
            request_id=request_id,
            event_time=event_time,
            fs_client=fs_client,
            is_anonymous=is_anonymous,
            shown_set=shown_set,
            shown_count=shown_count,
            session_id=session_id,
        )
    else:
        return _get_diverse_feed_legacy(
            user_id=user_id,
            device=device,
            final_feed_size=final_feed_size,
            request_id=request_id,
            event_time=event_time,
            fs_client=fs_client,
            is_anonymous=is_anonymous,
            shown_set=shown_set,
            shown_count=shown_count,
        )


async def _get_diverse_feed_embedding(
    user_id: str,
    device: str | None,
    final_feed_size: int,
    request_id: str,
    event_time: int,
    fs_client,
    is_anonymous: bool,
    shown_set: set,
    shown_count: int,
    session_id: str | None = None,
) -> FeedResponse:
    """
    Embedding-based retrieval using pgvector with session-aware personalization.

    Pipeline:
    1. Get session engagement data (if session_id provided)
    2. Determine cold-start stage
    3. Compute blended user embedding (long-term + session)
    4. Run parallel retrievers (embedding, fresh, trending, random)
    5. Score with Monolith (if enabled)
    6. Apply brand diversity
    7. Return feed with session metadata
    """
    # Initialize async clients
    async_pg = AsyncPostgresClient.from_settings(settings)
    async_redis = get_async_redis_client(settings)

    try:
        # Get total catalog size (sync call is fine here)
        pg_client = PostgresClient.from_settings(settings)
        total_products = pg_client.get_total_product_count()

        # Get liked product IDs for user embedding (empty for anonymous)
        liked_product_ids = []
        if not is_anonymous:
            liked_product_ids = get_user_liked_product_ids(fs_client, user_id)

        # Get session engagement data (if session_id provided)
        session_product_ids = []
        engagement_scores = {}
        if session_id and not is_anonymous:
            session_product_ids = get_session_positive_products(
                user_id, session_id, min_score=0.3, limit=30
            )
            engagement_scores = get_session_engagement_map(user_id, session_id)

        # Determine cold-start stage
        cold_start_stage = determine_cold_start_stage(
            len(liked_product_ids),
            len(session_product_ids)
        )

        logger.info(
            f"User {user_id}: stage={cold_start_stage}, "
            f"likes={len(liked_product_ids)}, session_positive={len(session_product_ids)}"
        )

        # Check if we should use two-tower learned embeddings
        use_learned = settings.use_learned_embeddings

        # Try to get user embedding from Monolith user tower (two-tower model)
        monolith_user_vec = None
        if use_learned and settings.monolith_enabled and not is_anonymous:
            try:
                monolith_client = get_monolith_client(settings)
                monolith_user_vec = monolith_client.get_user_embedding(user_id)
                logger.info(f"Got user tower embedding for user={user_id}, shape={monolith_user_vec.shape}")
            except Exception as e:
                logger.error(f"Monolith user tower failed: {e}, falling back to averaged embedding")

        # STEP 1: Compute user embedding based on cold-start stage
        if is_anonymous:
            # Anonymous user - non-personalized
            logger.info(f"Using anonymous retrieval for user={user_id}")
            candidates = await get_candidates_for_anonymous(
                async_pg, async_redis, total_limit=500
            )
        elif cold_start_stage == STAGE_BRAND_NEW:
            # Brand new user - exploration mode
            logger.info("Cold-start BRAND_NEW: exploration mode")
            candidates = await get_candidates_for_anonymous(
                async_pg, async_redis, total_limit=500
            )
        elif cold_start_stage == STAGE_BROWSING:
            # Has session signals but no likes - use session embedding
            logger.info("Cold-start BROWSING: using session signals only")
            session_emb = await get_session_embedding(
                async_pg, session_product_ids, engagement_scores,
                use_learned=use_learned,
            )
            if session_emb is not None:
                candidates = await get_candidates_parallel(
                    async_pg, async_redis, session_emb, shown_set, total_limit=500,
                    use_learned=use_learned,
                )
            else:
                candidates = await get_candidates_for_anonymous(
                    async_pg, async_redis, total_limit=500
                )
        else:
            # Has likes - use Monolith user tower or blended embedding
            if monolith_user_vec is not None:
                # Two-tower: user tower provides the embedding directly
                logger.info(f"Using Monolith user tower embedding for retrieval")
                user_embedding = monolith_user_vec
            else:
                # Fallback: average of liked product embeddings
                logger.info(
                    f"Using blended embedding: {len(liked_product_ids)} likes + "
                    f"{len(session_product_ids)} session products"
                )
                user_embedding = await get_blended_user_embedding(
                    async_pg,
                    liked_product_ids,
                    session_product_ids,
                    engagement_scores,
                    use_learned=use_learned,
                )

            # STEP 2: Parallel retrieval (~10ms)
            candidates = await get_candidates_parallel(
                async_pg, async_redis, user_embedding, shown_set, total_limit=500,
                use_learned=use_learned,
            )

        logger.info(f"Retrieved {len(candidates)} candidates via embedding retrieval")

        # Filter to unseen candidates
        candidates_unseen = [pid for pid in candidates if pid not in shown_set]

        # Calculate pagination metadata
        has_more = len(candidates_unseen) > final_feed_size
        unseen_count = len(candidates_unseen)

        # Determine tier
        if shown_count < 100:
            tier = 1
        elif shown_count < 500:
            tier = 2
        elif shown_count < 2000:
            tier = 3
        else:
            tier = 4

        # STEP 3: Score with Monolith or publish FeatureEvent
        # When using two-tower learned embeddings, retrieval order IS the ranking
        # (no separate scoring step needed). Only publish FeatureEvent for training.
        if use_learned and monolith_user_vec is not None:
            # Two-tower mode: retrieval order = ranking order
            # Publish FeatureEvent with user tower embedding for online training
            if settings.kafka_enabled and not is_anonymous:
                try:
                    kafka_producer = get_kafka_producer(settings)
                    feature_data = {
                        "user_id": user_id,
                        "session_id": session_id,
                        "user_embedding": monolith_user_vec.tolist(),
                        "context": {
                            "session_position": len(session_product_ids),
                            "hour_of_day": time.localtime().tm_hour,
                            "day_of_week": time.localtime().tm_wday,
                            "device": device or "unknown",
                            "cold_start_stage": cold_start_stage,
                        },
                        "candidates": [
                            {"product_id": pid, "position": i}
                            for i, pid in enumerate(candidates_unseen[:20])
                        ]
                    }
                    kafka_producer.publish_feature_event(request_id, event_time, feature_data)
                    logger.info(f"Published FeatureEvent (two-tower) for request_id={request_id}")
                except Exception as e:
                    logger.error(f"Failed to publish FeatureEvent: {e}")

        elif settings.monolith_enabled and not is_anonymous and candidates_unseen:
            # Legacy mode: separate Monolith scoring step
            try:
                candidates_to_score = candidates_unseen[:500]
                monolith_client = get_monolith_client(settings)
                user_emb, product_embs, scores = monolith_client.predict(
                    user_id=user_id,
                    product_ids=candidates_to_score
                )

                if settings.kafka_enabled:
                    try:
                        kafka_producer = get_kafka_producer(settings)
                        feature_data = {
                            "user_id": user_id,
                            "session_id": session_id,
                            "user_embedding": user_emb.tolist(),
                            "context": {
                                "session_position": len(session_product_ids),
                                "hour_of_day": time.localtime().tm_hour,
                                "day_of_week": time.localtime().tm_wday,
                                "device": device or "unknown",
                                "cold_start_stage": cold_start_stage,
                            },
                            "candidates": [
                                {
                                    "product_id": pid,
                                    "product_embedding": product_embs[pid].tolist(),
                                    "monolith_score": scores[pid],
                                    "position": i
                                }
                                for i, pid in enumerate(list(scores.keys())[:20])
                            ]
                        }
                        kafka_producer.publish_feature_event(request_id, event_time, feature_data)
                        logger.info(f"Published FeatureEvent for request_id={request_id}")
                    except Exception as e:
                        logger.error(f"Failed to publish FeatureEvent: {e}")

                ranked_candidates = sorted(
                    [(pid, scores.get(pid, 0)) for pid in candidates_to_score],
                    key=lambda x: x[1],
                    reverse=True
                )
                candidates_unseen = [pid for pid, _ in ranked_candidates]
                logger.info(f"Monolith ranked {len(candidates_unseen)} candidates")

            except Exception as e:
                logger.error(f"Monolith prediction failed: {e}")

        # STEP 4: Fetch metadata and apply brand diversity (~5ms)
        # Take extra candidates for diversity filtering
        candidates_for_diversity = candidates_unseen[:final_feed_size * 3]
        product_metadata = await async_pg.get_product_metadata_for_ids(candidates_for_diversity)

        # Build brand map for diversity enforcement
        brand_map = {pid: meta.get("brand") for pid, meta in product_metadata.items()}

        # Apply brand diversity (max 2-3 items per brand in final feed)
        final_ids = enforce_brand_diversity(candidates_for_diversity, brand_map, max_per_window=3)
        final_ids = final_ids[:final_feed_size]

        # Record shown (skip for anonymous users)
        if not is_anonymous:
            add_shown_items_fs(fs_client, user_id, final_ids)

        # Build feed response
        items = [
            ProductItem(
                id=pid,
                title=meta.get("title"),
                price=meta.get("price"),
                compare_at_price=meta.get("compare_at_price"),
                images=meta.get("images", []),
                image_has_text=meta.get("image_has_text"),
                category=meta.get("category"),
                subcategory=meta.get("subcategory"),
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

        return FeedResponse(
            feed=items,
            request_id=request_id,
            has_more=has_more,
            unseen_count=unseen_count,
            shown_count=shown_count,
            total_count=total_products,
            tier=tier,
            # Session metadata
            session_id=session_id,
            cold_start_stage=cold_start_stage,
            session_positive_count=len(session_product_ids),
        )

    finally:
        # Clean up async clients
        await async_redis.close()


def _get_diverse_feed_legacy(
    user_id: str,
    device: str | None,
    final_feed_size: int,
    request_id: str,
    event_time: int,
    fs_client,
    is_anonymous: bool,
    shown_set: set,
    shown_count: int,
) -> FeedResponse:
    """
    Legacy heuristic-based candidate generation.

    This is the original implementation kept for fallback.
    """
    pg_client = PostgresClient.from_settings(settings)

    # Get total catalog size
    total_products = pg_client.get_total_product_count()

    # Generate dynamic personalized candidate pool
    candidates_raw = get_dynamic_personalized_pool(
        pg_client,
        fs_client,
        user_id,
        shown_set,
        shown_count,
        is_anonymous,
        settings
    )

    # Filter to unseen candidates
    candidates_unseen = [pid for pid in candidates_raw if pid not in shown_set]

    # Calculate pagination metadata
    has_more = len(candidates_unseen) > final_feed_size
    unseen_count = len(candidates_unseen)

    # Determine tier
    if shown_count < 100:
        tier = 1
    elif shown_count < 500:
        tier = 2
    elif shown_count < 2000:
        tier = 3
    else:
        tier = 4

    # Step 3: Score candidates with Monolith (if enabled)
    if settings.monolith_enabled and not is_anonymous and candidates_unseen:
        try:
            candidates_to_score = candidates_unseen[:500]
            monolith_client = get_monolith_client(settings)
            user_emb, product_embs, scores = monolith_client.predict(
                user_id=user_id,
                product_ids=candidates_to_score
            )

            if settings.kafka_enabled:
                try:
                    kafka_producer = get_kafka_producer(settings)
                    feature_data = {
                        "user_id": user_id,
                        "user_embedding": user_emb.tolist(),
                        "context": {
                            "session_position": 1,
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
                            for i, pid in enumerate(list(scores.keys())[:20])
                        ]
                    }
                    kafka_producer.publish_feature_event(request_id, event_time, feature_data)
                    logger.info(f"Published FeatureEvent for request_id={request_id}")
                except Exception as e:
                    logger.error(f"Failed to publish FeatureEvent: {e}")

            personal_scores = scores
            logger.info(f"Monolith predictions successful: {len(personal_scores)} products scored")

        except Exception as e:
            logger.error(f"Monolith prediction failed: {e}, falling back to stub scoring")
            features = fetch_features_for_ids(candidates_unseen)
            personal_scores = score_with_model_or_fallback(features, fallback_scores=None)
    else:
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
    freshness_metrics = fetch_freshness_metrics(recent_unseen, pg=pg_client)
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

    # Collect all candidate IDs from slices for metadata fetch
    all_slice_ids = (
        slices.get("personal", []) +
        slices.get("category", []) +
        slices.get("fresh", [])
    )

    # Hydrate metadata from Postgres (needed for brand diversity)
    product_metadata: Dict[str, dict] = join_product_metadata(pg_client, all_slice_ids)

    # Build brand map for diversity enforcement
    brand_map = {pid: meta.get("brand") for pid, meta in product_metadata.items()}

    # Step 7.3: interleave with brand diversity
    final_ids = interleave_buckets(slices, final_feed_size, brand_map=brand_map)

    # Step 8: record shown (skip for anonymous users)
    if not is_anonymous:
        add_shown_items_fs(fs_client, user_id, final_ids)

    # Build feed response with full product details
    items = [
        ProductItem(
            id=pid,
            title=meta.get("title"),
            price=meta.get("price"),
            compare_at_price=meta.get("compare_at_price"),
            images=meta.get("images", []),
            image_has_text=meta.get("image_has_text"),
            category=meta.get("category"),
            subcategory=meta.get("subcategory"),
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

    return FeedResponse(
        feed=items,
        request_id=request_id,
        has_more=has_more,
        unseen_count=unseen_count,
        shown_count=shown_count,
        total_count=total_products,
        tier=tier
    )


@app.get("/search", response_model=SearchResponse)
async def search(
    q: str = Query(..., description="Search query"),
    limit: int = Query(20, ge=1, le=100, description="Number of results to return"),
    mode: str = Query("hybrid", description="Search mode: hybrid, semantic, visual, keyword"),
    weights: str = Query("1,1,1", description="Comma-separated weights: text,image,keyword"),
) -> SearchResponse:
    """
    Hybrid product search combining semantic, visual, and keyword search.

    Modes:
    - hybrid: Combines all three search methods using RRF fusion (default)
    - semantic: Text embedding similarity search (1024-dim Marqo embeddings)
    - visual: Cross-modal text-to-image search (512-dim CLIP embeddings)
    - keyword: PostgreSQL full-text search (tsvector/BM25-style ranking)

    Weights control the relative importance of each search method in hybrid mode.
    Format: "text_weight,image_weight,keyword_weight" (e.g., "1,1,1" for equal)
    """
    # Parse weights
    try:
        weight_parts = weights.split(",")
        if len(weight_parts) != 3:
            raise ValueError("Expected 3 weights")
        weight_tuple = tuple(float(w) for w in weight_parts)
    except (ValueError, TypeError) as e:
        logger.warning(f"Invalid weights '{weights}', using defaults: {e}")
        weight_tuple = settings.search_default_weights

    # Validate mode
    valid_modes = {"hybrid", "semantic", "visual", "keyword"}
    if mode not in valid_modes:
        logger.warning(f"Invalid mode '{mode}', defaulting to 'hybrid'")
        mode = "hybrid"

    # Initialize clients
    async_pg = AsyncPostgresClient.from_settings(settings)
    pg_client = PostgresClient.from_settings(settings)
    embedding_service = EmbeddingService(settings.embedding_service_url)

    # Create searcher
    searcher = HybridSearcher(
        pg_client=async_pg,
        embedding_service=embedding_service,
        rrf_k=settings.search_rrf_k,
        candidate_multiplier=settings.search_candidate_multiplier,
    )

    # Execute search
    product_ids, latency_ms = await searcher.search(
        query=q,
        limit=limit,
        mode=mode,
        weights=weight_tuple,
    )

    # Fetch product metadata
    if product_ids:
        product_metadata = await async_pg.get_product_metadata_for_ids(product_ids)
    else:
        product_metadata = {}

    # Build response items (preserve search order)
    items = [
        ProductItem(
            id=pid,
            title=meta.get("title"),
            price=meta.get("price"),
            compare_at_price=meta.get("compare_at_price"),
            images=meta.get("images", []),
            image_has_text=meta.get("image_has_text"),
            category=meta.get("category"),
            subcategory=meta.get("subcategory"),
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

    logger.info(f"Search completed: query='{q}', mode={mode}, results={len(items)}, latency={latency_ms:.1f}ms")

    return SearchResponse(
        query=q,
        results=items,
        total=len(items),
        mode=mode,
        latency_ms=latency_ms,
    )


@app.post("/track", response_model=TrackResponse)
def track_interaction(request: TrackRequest) -> TrackResponse:
    """
    Track user interaction with session-aware engagement scoring.

    Records interaction in in-memory session store for real-time
    recommendation adaptation, and publishes to Kafka for offline training.

    Session features:
    - Revisit detection (scrolling back to a product)
    - Engagement score computation with correct signal weights
    - Position history tracking
    """
    # Ignore anonymous users
    if request.user_id == "anonymous":
        return TrackResponse(
            status="ignored",
            request_id=request.request_id,
            message="Anonymous users not tracked"
        )

    event_time = int(time.time() * 1000)
    engagement = None
    is_revisit = False

    # Record in session store (if session_id provided)
    if request.session_id:
        try:
            engagement = session_record_view(
                user_id=request.user_id,
                session_id=request.session_id,
                product_id=request.product_id,
                position=request.position,
                dwell_time=request.dwell_time,
                images_viewed=request.images_viewed,
                action=request.action,
            )
            is_revisit = engagement.is_revisit

            logger.debug(
                f"Session recorded: user={request.user_id}, session={request.session_id}, "
                f"product={request.product_id}, score={engagement.engagement_score:.2f}, "
                f"is_revisit={is_revisit}"
            )
        except Exception as e:
            logger.warning(f"Failed to record session: {e}")

    # Compute label based on CORRECTED signal interpretation
    # NOTE: swipe_up is NOT positive - it's just moving on
    # Positive signals: like, collection_add, shop_now, revisit with engagement
    if engagement:
        # Use engagement score from session store
        label = max(0.0, engagement.engagement_score)
    else:
        # Fallback if no session: only explicit positive actions
        explicit_positive = {"like", "collection_add", "shop_now"}
        label = 1.0 if request.action in explicit_positive else 0.0

    # Publish to Kafka (if enabled)
    if settings.kafka_enabled:
        action_data = {
            "user_id": request.user_id,
            "session_id": request.session_id,
            "product_id": request.product_id,
            "action": request.action,
            "dwell_time": request.dwell_time,
            "images_viewed": request.images_viewed,
            "position": request.position,
            "is_revisit": is_revisit,
            "engagement_score": engagement.engagement_score if engagement else 0.0,
            "label": label,
        }

        try:
            kafka_producer = get_kafka_producer(settings)
            kafka_producer.publish_action_event(request.request_id, event_time, action_data)

            logger.info(
                f"Published ActionEvent: request_id={request.request_id}, "
                f"user={request.user_id}, product={request.product_id}, "
                f"action={request.action}, score={action_data['engagement_score']:.2f}"
            )
        except Exception as e:
            logger.error(f"Failed to publish ActionEvent: {e}")
            # Don't fail the request if Kafka publish fails

    return TrackResponse(
        status="tracked",
        request_id=request.request_id,
        message=f"Engagement score: {engagement.engagement_score:.2f}" if engagement else "Tracked"
    )
