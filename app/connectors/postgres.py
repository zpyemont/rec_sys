from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence
import contextlib

import psycopg2  # type: ignore
import psycopg2.extras  # type: ignore

from ..settings import Settings


class PostgresClient:
    def __init__(self, dsn: str):
        self._dsn = dsn

    @classmethod
    def from_settings(cls, settings: Settings) -> "PostgresClient":
        if settings.postgres_dsn:
            dsn = settings.postgres_dsn
        else:
            host = settings.pg_host or "localhost"
            port = settings.pg_port or 5432
            user = settings.pg_user or "postgres"
            password = settings.pg_password or ""
            database = settings.pg_database or "product"  # Default to 'product' database used by ingestion_pipeline
            dsn = f"host={host} port={port} user={user} password={password} dbname={database}"
        return cls(dsn)

    @contextlib.contextmanager
    def _get_conn(self):
        conn = psycopg2.connect(self._dsn)
        try:
            yield conn
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def fetch_all(self, sql: str, params: Sequence[Any] | None = None) -> List[Dict[str, Any]]:
        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params or [])
                rows = cur.fetchall()
                return [dict(r) for r in rows]

    def fetch_val_list(self, sql: str, params: Sequence[Any] | None = None, col: str = "product_id") -> List[str]:
        rows = self.fetch_all(sql, params)
        return [str(r.get(col)) for r in rows if r.get(col) is not None]

    # Specific helpers against products table
    def get_recent_products(self, hours: int, limit: int) -> List[str]:
        sql = (
            "SELECT product_id FROM products "
            "WHERE parsed_at >= NOW() - INTERVAL '%s hours' "
            "ORDER BY parsed_at DESC NULLS LAST LIMIT %s"
        )
        return self.fetch_val_list(sql, (hours, limit))

    def get_popular_products(self, limit: int) -> List[str]:
        # Placeholder popularity = latest updated_at
        sql = (
            "SELECT product_id FROM products "
            "ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST LIMIT %s"
        )
        return self.fetch_val_list(sql, (limit,))

    def get_by_brand_or_vendor(self, cat: str, limit: int) -> List[str]:
        sql = (
            "SELECT product_id FROM products "
            "WHERE LOWER(brand) = LOWER(%s) OR LOWER(vendor) = LOWER(%s) "
            "ORDER BY updated_at DESC NULLS LAST LIMIT %s"
        )
        return self.fetch_val_list(sql, (cat, cat, limit))

    def get_product_metadata_for_ids(self, prod_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch full product metadata for given product IDs.
        Returns: List of dicts with all product fields needed by frontend.
        """
        if not prod_ids:
            return []
        # Use ANY with array param to avoid SQL injection if list is large
        sql = (
            "SELECT product_id, title, price, images, category, like_count, "
            "description, url, brand, created_at, currency, availability "
            "FROM products WHERE product_id = ANY(%s)"
        )
        rows = self.fetch_all(sql, (prod_ids,))
        return rows

    def increment_like_count(self, product_id: str) -> int:
        """
        Increment like count for a product and return new count.
        """
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE products
                    SET like_count = like_count + 1,
                        updated_at = NOW()
                    WHERE product_id = %s
                    RETURNING like_count
                    """,
                    (product_id,)
                )
                result = cur.fetchone()
                conn.commit()
                return result[0] if result else 0

    def decrement_like_count(self, product_id: str) -> int:
        """
        Decrement like count for a product (min 0) and return new count.
        """
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE products
                    SET like_count = GREATEST(like_count - 1, 0),
                        updated_at = NOW()
                    WHERE product_id = %s
                    RETURNING like_count
                    """,
                    (product_id,)
                )
                result = cur.fetchone()
                conn.commit()
                return result[0] if result else 0
