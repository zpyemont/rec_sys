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


def join_product_metadata(pg: PostgresClient, prod_ids: List[str]) -> Dict[str, dict]:
    """
    Fetch full product metadata and return as dict keyed by product_id.
    Maps PostgreSQL schema to frontend-expected format.
    """
    settings = get_settings()
    rows = pg.get_product_metadata_for_ids(prod_ids)

    result = {}
    for row in rows:
        result[row["product_id"]] = {
            "id": row["product_id"],  # Rename product_id to id
            "title": row.get("title"),
            "price": float(row["price"]) if row.get("price") else None,
            "images": row.get("images") or [],  # Ensure it's an array
            "category": row.get("category"),
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
                    "images": row.get("images") or [],
                    "category": row.get("category"),
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
