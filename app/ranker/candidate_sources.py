from __future__ import annotations

from typing import Dict, List, Any
from ..connectors.postgres import PostgresClient
from ..settings import get_settings


def query_popular_ids(pg: PostgresClient, limit: int = 5000) -> List[str]:
    return pg.get_popular_products(limit=limit)


def query_recent_ids(pg: PostgresClient, hours: int = 24, limit: int = 1000) -> List[str]:
    return pg.get_recent_products(hours=hours, limit=limit)


def query_top_by_category(pg: PostgresClient, cat: str, limit: int = 200) -> List[str]:
    return pg.get_by_brand_or_vendor(cat=cat, limit=limit)


def fetch_freshness_metrics(prod_ids: List[str]) -> Dict[str, float]:
    return {str(pid): 1.0 for pid in prod_ids}


def fetch_features_for_ids(prod_ids: List[str]) -> Dict[str, Any]:
    return {str(pid): {} for pid in prod_ids}


def get_user_liked_product_ids(fs_client, user_id: str) -> List[str]:
    """Extract product IDs from user's Firestore likes"""
    if not fs_client:
        return []

    try:
        likes_ref = fs_client.client.collection("users").document(user_id).collection("likes")
        likes_docs = likes_ref.stream()

        product_ids = []
        for doc in likes_docs:
            data = doc.to_dict() if hasattr(doc, 'to_dict') else {}
            if data and data.get("type") == "product" and doc.id.startswith("product_"):
                product_id = doc.id.replace("product_", "")
                product_ids.append(product_id)

        return product_ids
    except Exception:
        return []


def get_user_behavioral_profile(fs_client, pg: PostgresClient, user_id: str) -> Dict[str, Any]:
    """
    Build user profile from Firestore likes + PostgreSQL metadata.

    Returns:
        {
            'liked_products': [product_ids],
            'preferred_categories': [top 5 categories],
            'preferred_brands': [top 5 brands],
            'price_range': {'min': float, 'max': float, 'avg': float},
            'total_likes': int
        }
    """
    # Get liked products from Firestore
    liked_product_ids = get_user_liked_product_ids(fs_client, user_id)

    if not liked_product_ids:
        return {
            'liked_products': [],
            'preferred_categories': [],
            'preferred_brands': [],
            'price_range': None,
            'total_likes': 0
        }

    # Get metadata for liked products
    liked_metadata = pg.get_product_metadata_for_ids(liked_product_ids)

    # Extract preferences - prioritize subcategory over category
    categories = []
    for m in liked_metadata:
        if m.get('subcategory'):
            categories.append(m['subcategory'])
        elif m.get('category'):
            categories.append(m['category'])

    brands = [m['brand'] for m in liked_metadata if m.get('brand')]
    prices = [float(m['price']) for m in liked_metadata if m.get('price') and m['price']]

    # Calculate preferred price range
    price_range = None
    if prices:
        avg_price = sum(prices) / len(prices)
        price_range = {
            'min': min(prices) * 0.5,
            'max': max(prices) * 2.0,
            'avg': avg_price
        }

    # Count frequencies
    from collections import Counter
    category_counts = Counter(categories)
    brand_counts = Counter(brands)

    return {
        'liked_products': liked_product_ids,
        'preferred_categories': [cat for cat, _ in category_counts.most_common(5)],
        'preferred_brands': [brand for brand, _ in brand_counts.most_common(5)],
        'price_range': price_range,
        'total_likes': len(liked_product_ids)
    }


def get_dynamic_personalized_pool(
    pg: PostgresClient,
    fs_client,
    user_id: str,
    shown_set,
    shown_count: int,
    is_anonymous: bool,
    settings
) -> List[str]:
    """
    Generate personalized candidate pool with FRESHNESS FIRST and strong exploration.

    Strategy:
    1. Fresh products (30%) - ALL categories
    2. Behavioral personalization (40%)
    3. Adjacent exploration (15%)
    4. Trending (10%)
    5. Serendipity (5%)
    """
    import logging
    logger = logging.getLogger(__name__)

    # Determine tier and pool size
    if shown_count < 100:
        pool_size = settings.tier_1_pool_size
        tier = 1
    elif shown_count < 500:
        pool_size = settings.tier_2_pool_size
        tier = 2
    elif shown_count < 2000:
        pool_size = settings.tier_3_pool_size
        tier = 3
    else:
        pool_size = 100000  # Pagination mode
        tier = 4

    logger.info(f"User {user_id}: Tier {tier}, shown_count={shown_count}, pool_size={pool_size}")

    all_candidates = []

    # ═══════════════════════════════════════════════════════════
    # 1. FRESH PRODUCTS (30%) - ALWAYS, ALL CATEGORIES
    # ═══════════════════════════════════════════════════════════
    fresh_limit = int(pool_size * 0.30)
    try:
        # Use diverse query for anonymous users to prevent brand domination
        if is_anonymous:
            fresh_candidates = pg.get_recent_products_diverse(
                hours=48,
                limit=fresh_limit,
                max_per_brand=10
            )
        else:
            fresh_candidates = pg.get_recent_products(
                hours=48,
                limit=fresh_limit
            )
        all_candidates.extend([('fresh', pid) for pid in fresh_candidates])
        logger.info(f"✨ Added {len(fresh_candidates)} fresh products (ALL categories)")
    except Exception as e:
        logger.warning(f"Fresh products failed: {e}")

    # ═══════════════════════════════════════════════════════════
    # 2. BEHAVIORAL PERSONALIZATION (40%)
    # ═══════════════════════════════════════════════════════════
    profile = None
    if not is_anonymous:
        try:
            profile = get_user_behavioral_profile(fs_client, pg, user_id)
            behavioral_limit = int(pool_size * 0.40)
            behavioral_candidates = []

            # From preferred categories
            if profile['preferred_categories']:
                cat_candidates = pg.get_candidates_from_categories(
                    profile['preferred_categories'],
                    profile['price_range'],
                    limit=int(behavioral_limit * 0.6)
                )
                behavioral_candidates.extend(cat_candidates)

            # From preferred brands
            if profile['preferred_brands']:
                brand_candidates = pg.get_candidates_from_brands(
                    profile['preferred_brands'],
                    profile['price_range'],
                    limit=int(behavioral_limit * 0.4)
                )
                behavioral_candidates.extend(brand_candidates)

            all_candidates.extend([('behavioral', pid) for pid in behavioral_candidates])
            logger.info(f"🎯 Added {len(behavioral_candidates)} behavioral matches")
        except Exception as e:
            logger.warning(f"Behavioral candidates failed: {e}")

    # ═══════════════════════════════════════════════════════════
    # 3. ADJACENT EXPLORATION (15%) - Expand interests
    # ═══════════════════════════════════════════════════════════
    if not is_anonymous and profile and profile['preferred_categories']:
        adjacent_limit = int(pool_size * 0.15)
        try:
            adjacent_candidates = pg.get_adjacent_categories(
                profile['preferred_categories'],
                settings.category_adjacency,
                limit=adjacent_limit
            )
            all_candidates.extend([('adjacent', pid) for pid in adjacent_candidates])
            logger.info(f"🔍 Added {len(adjacent_candidates)} adjacent categories")
        except Exception as e:
            logger.warning(f"Adjacent candidates failed: {e}")

    # ═══════════════════════════════════════════════════════════
    # 4. TRENDING (10%) - Viral products
    # ═══════════════════════════════════════════════════════════
    trending_limit = int(pool_size * 0.10)
    try:
        trending_candidates = pg.get_trending_products(
            limit=trending_limit,
            hours=48
        )
        all_candidates.extend([('trending', pid) for pid in trending_candidates])
        logger.info(f"🔥 Added {len(trending_candidates)} trending products")
    except Exception as e:
        logger.warning(f"Trending candidates failed: {e}")

    # ═══════════════════════════════════════════════════════════
    # 5. SERENDIPITY (5%) - Random discovery
    # ═══════════════════════════════════════════════════════════
    random_limit = int(pool_size * 0.05)
    try:
        random_candidates = pg.get_random_high_quality(limit=random_limit)
        all_candidates.extend([('serendipity', pid) for pid in random_candidates])
        logger.info(f"🎲 Added {len(random_candidates)} random products")
    except Exception as e:
        logger.warning(f"Serendipity candidates failed: {e}")

    # ═══════════════════════════════════════════════════════════
    # DEDUPLICATE & FILTER SHOWN
    # ═══════════════════════════════════════════════════════════
    seen = set()
    final = []
    source_counts = {}

    for source, pid in all_candidates:
        if pid not in seen and pid not in shown_set:
            seen.add(pid)
            final.append(pid)
            source_counts[source] = source_counts.get(source, 0) + 1

    logger.info(f"📊 Final pool: {len(final)} products from {source_counts}")

    # If pool is too small, fill with popular
    if len(final) < 1000:
        try:
            # Use diverse query for anonymous users to prevent brand domination
            if is_anonymous:
                popular_candidates = pg.get_popular_products_diverse(
                    limit=pool_size,
                    max_per_brand=10
                )
            else:
                popular_candidates = pg.get_popular_products(limit=pool_size)
            for pid in popular_candidates:
                if pid not in seen and pid not in shown_set:
                    seen.add(pid)
                    final.append(pid)
                    if len(final) >= pool_size:
                        break
            logger.info(f"Filled pool with popular items, now {len(final)} total")
        except Exception as e:
            logger.warning(f"Popular fallback failed: {e}")

    return final


def _parse_pg_boolean_array(value) -> List[bool] | None:
    """
    Parse PostgreSQL boolean array string to Python list.
    e.g., "{f,t,t}" -> [False, True, True]
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"_parse_pg_boolean_array called with value: {value}, type: {type(value)}")

    if value is None:
        logger.info("Value is None, returning None")
        return None
    if isinstance(value, list):
        logger.info(f"Value is already a list: {value}")
        return value  # Already a list
    if isinstance(value, str):
        logger.info(f"Value is a string, parsing: {value}")
        # Remove curly braces and split by comma
        value = value.strip('{}')
        if not value:
            return []
        result = [item.strip().lower() == 't' for item in value.split(',')]
        logger.info(f"Parsed result: {result}")
        return result
    logger.warning(f"Unexpected type for image_has_text: {type(value)}")
    return None


def join_product_metadata(pg: PostgresClient, prod_ids: List[str]) -> Dict[str, dict]:
    """
    Fetch full product metadata and return as dict keyed by product_id.
    Maps PostgreSQL schema to frontend-expected format.
    """
    import logging
    logger = logging.getLogger(__name__)

    settings = get_settings()
    rows = pg.get_product_metadata_for_ids(prod_ids)

    logger.info(f"join_product_metadata: Processing {len(rows)} rows")

    result = {}
    for row in rows:
        logger.info(f"Raw row image_has_text for {row['product_id']}: {row.get('image_has_text')}, type: {type(row.get('image_has_text'))}")
        result[row["product_id"]] = {
            "id": row["product_id"],  # Rename product_id to id
            "title": row.get("title"),
            "price": float(row["price"]) if row.get("price") else None,
            "compare_at_price": float(row["compare_at_price"]) if row.get("compare_at_price") else None,
            "images": row.get("images") or [],  # Ensure it's an array
            "image_has_text": _parse_pg_boolean_array(row.get("image_has_text")),  # Parse PostgreSQL boolean array
            "category": row.get("category"),
            "subcategory": row.get("subcategory"),
            "like_count": row.get("like_count", 0),
            "description": row.get("description"),
            "url": row.get("url"),
            "brand": row.get("brand"),
            "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
            "currency": row.get("currency"),
            "availability": row.get("availability")
        }

    if result:
        return result

    # Fallback to BigQuery if configured
    try:
        from ..connectors.bigquery import BigQueryClient

        if settings.bq_dataset and settings.bq_table_products:
            bq = BigQueryClient.from_settings(settings)
            bq_rows = bq.get_product_metadata_for_ids(
                dataset=settings.bq_dataset,
                table=settings.bq_table_products,
                prod_ids=prod_ids,
            )
            # Map BigQuery results to same format
            for row in bq_rows:
                result[row["product_id"]] = {
                    "id": row["product_id"],
                    "title": row.get("title"),
                    "price": float(row["price"]) if row.get("price") else None,
                    "compare_at_price": float(row["compare_at_price"]) if row.get("compare_at_price") else None,
                    "images": row.get("images") or [],
                    "image_has_text": _parse_pg_boolean_array(row.get("image_has_text")),  # Parse boolean array
                    "category": row.get("category"),
                    "subcategory": row.get("subcategory"),
                    "like_count": row.get("like_count", 0),
                    "description": row.get("description"),
                    "url": row.get("url"),
                    "brand": row.get("brand"),
                    "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
                    "currency": row.get("currency"),
                    "availability": row.get("availability")
                }
    except Exception:
        pass

    return result
