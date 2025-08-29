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


def join_product_metadata(pg: PostgresClient, prod_ids: List[str]) -> List[Dict[str, Any]]:
    settings = get_settings()
    rows = pg.get_product_metadata_for_ids(prod_ids)
    if rows:
        return rows

    # Fallback to BigQuery if configured
    try:
        from ..connectors.bigquery import BigQueryClient

        if settings.bq_dataset and settings.bq_table_products:
            bq = BigQueryClient.from_settings(settings)
            return bq.get_product_metadata_for_ids(
                dataset=settings.bq_dataset,
                table=settings.bq_table_products,
                prod_ids=prod_ids,
            )
    except Exception:
        pass

    # Minimal fallback: return IDs only
    return [{"prod_id": str(pid), "title": None, "price": None, "image_url": None} for pid in prod_ids]
