from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence
import contextlib

import psycopg2  # type: ignore
import psycopg2.extras  # type: ignore

from ..settings import Settings


class PostgresClient:
    def __init__(self, dsn: str, settings: Optional[Settings] = None):
        self._dsn = dsn
        self._settings = settings

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
        return cls(dsn, settings)

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

    def _build_stock_filter_clause(self) -> tuple[str, list[str]]:
        """
        Build a SQL WHERE clause to filter out out-of-stock products.
        Returns: (where_clause, params) tuple
        """
        if not self._settings or not self._settings.filter_out_of_stock:
            return ("", [])

        excluded_values = self._settings.excluded_availability_values
        if not excluded_values:
            return ("", [])

        # Build clause: (availability IS NULL OR LOWER(availability) NOT IN (...))
        placeholders = ", ".join(["%s"] * len(excluded_values))
        where_clause = f"(availability IS NULL OR LOWER(availability) NOT IN ({placeholders}))"
        # Lowercase the excluded values for case-insensitive comparison
        params = [v.lower() for v in excluded_values]

        return (where_clause, params)

    # Specific helpers against products table
    def get_recent_products(self, hours: int, limit: int) -> List[str]:
        stock_filter, stock_params = self._build_stock_filter_clause()

        if stock_filter:
            sql = (
                "SELECT product_id FROM products "
                "WHERE parsed_at >= NOW() - INTERVAL '%s hours' "
                f"AND {stock_filter} "
                "ORDER BY parsed_at DESC NULLS LAST LIMIT %s"
            )
            params = [hours] + stock_params + [limit]
        else:
            sql = (
                "SELECT product_id FROM products "
                "WHERE parsed_at >= NOW() - INTERVAL '%s hours' "
                "ORDER BY parsed_at DESC NULLS LAST LIMIT %s"
            )
            params = [hours, limit]

        return self.fetch_val_list(sql, params)

    def get_popular_products(self, limit: int) -> List[str]:
        # Placeholder popularity = latest updated_at
        stock_filter, stock_params = self._build_stock_filter_clause()

        if stock_filter:
            sql = (
                "SELECT product_id FROM products "
                f"WHERE {stock_filter} "
                "ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST LIMIT %s"
            )
            params = stock_params + [limit]
        else:
            sql = (
                "SELECT product_id FROM products "
                "ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST LIMIT %s"
            )
            params = [limit]

        return self.fetch_val_list(sql, params)

    def get_by_brand_or_vendor(self, cat: str, limit: int) -> List[str]:
        stock_filter, stock_params = self._build_stock_filter_clause()

        if stock_filter:
            sql = (
                "SELECT product_id FROM products "
                "WHERE (LOWER(brand) = LOWER(%s) OR LOWER(vendor) = LOWER(%s)) "
                f"AND {stock_filter} "
                "ORDER BY updated_at DESC NULLS LAST LIMIT %s"
            )
            params = [cat, cat] + stock_params + [limit]
        else:
            sql = (
                "SELECT product_id FROM products "
                "WHERE LOWER(brand) = LOWER(%s) OR LOWER(vendor) = LOWER(%s) "
                "ORDER BY updated_at DESC NULLS LAST LIMIT %s"
            )
            params = [cat, cat, limit]

        return self.fetch_val_list(sql, params)

    def get_product_metadata_for_ids(self, prod_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch full product metadata for given product IDs.
        Returns: List of dicts with all product fields needed by frontend.
        """
        import logging
        logger = logging.getLogger(__name__)

        if not prod_ids:
            return []
        # Use ANY with array param to avoid SQL injection if list is large
        sql = (
            "SELECT product_id, title, price, images, category, like_count, "
            "description, url, brand, created_at, currency, availability, image_has_text "
            "FROM products WHERE product_id = ANY(%s)"
        )
        rows = self.fetch_all(sql, (prod_ids,))

        # Log first row to debug
        if rows:
            logger.info(f"PostgreSQL returned {len(rows)} rows")
            logger.info(f"First row keys: {rows[0].keys()}")
            logger.info(f"First row image_has_text: {rows[0].get('image_has_text')}, type: {type(rows[0].get('image_has_text'))}")

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

    def get_total_product_count(self) -> int:
        """Get total count of in-stock products"""
        stock_filter, stock_params = self._build_stock_filter_clause()

        if stock_filter:
            sql = f"SELECT COUNT(*) as count FROM products WHERE {stock_filter}"
            params = stock_params
        else:
            sql = "SELECT COUNT(*) as count FROM products"
            params = []

        rows = self.fetch_all(sql, params)
        return rows[0]['count'] if rows else 0

    def get_candidates_from_categories(
        self,
        categories: List[str],
        price_range: Optional[Dict],
        limit: int
    ) -> List[str]:
        """Get products in specific categories, optionally filtered by price"""
        if not categories:
            return []

        stock_filter, stock_params = self._build_stock_filter_clause()

        if price_range and price_range.get('min') and price_range.get('max'):
            if stock_filter:
                sql = f"""
                    SELECT product_id
                    FROM products
                    WHERE category = ANY(%s)
                      AND price BETWEEN %s AND %s
                      AND {stock_filter}
                    ORDER BY like_count DESC, updated_at DESC
                    LIMIT %s
                """
                params = [categories, price_range['min'], price_range['max']] + stock_params + [limit]
            else:
                sql = """
                    SELECT product_id
                    FROM products
                    WHERE category = ANY(%s)
                      AND price BETWEEN %s AND %s
                    ORDER BY like_count DESC, updated_at DESC
                    LIMIT %s
                """
                params = [categories, price_range['min'], price_range['max'], limit]
        else:
            if stock_filter:
                sql = f"""
                    SELECT product_id
                    FROM products
                    WHERE category = ANY(%s)
                      AND {stock_filter}
                    ORDER BY like_count DESC, updated_at DESC
                    LIMIT %s
                """
                params = [categories] + stock_params + [limit]
            else:
                sql = """
                    SELECT product_id
                    FROM products
                    WHERE category = ANY(%s)
                    ORDER BY like_count DESC, updated_at DESC
                    LIMIT %s
                """
                params = [categories, limit]

        return self.fetch_val_list(sql, params)

    def get_candidates_from_brands(
        self,
        brands: List[str],
        price_range: Optional[Dict],
        limit: int
    ) -> List[str]:
        """Get products from specific brands, optionally filtered by price"""
        if not brands:
            return []

        stock_filter, stock_params = self._build_stock_filter_clause()

        if price_range and price_range.get('min') and price_range.get('max'):
            if stock_filter:
                sql = f"""
                    SELECT product_id
                    FROM products
                    WHERE brand = ANY(%s)
                      AND price BETWEEN %s AND %s
                      AND {stock_filter}
                    ORDER BY updated_at DESC, like_count DESC
                    LIMIT %s
                """
                params = [brands, price_range['min'], price_range['max']] + stock_params + [limit]
            else:
                sql = """
                    SELECT product_id
                    FROM products
                    WHERE brand = ANY(%s)
                      AND price BETWEEN %s AND %s
                    ORDER BY updated_at DESC, like_count DESC
                    LIMIT %s
                """
                params = [brands, price_range['min'], price_range['max'], limit]
        else:
            if stock_filter:
                sql = f"""
                    SELECT product_id
                    FROM products
                    WHERE brand = ANY(%s)
                      AND {stock_filter}
                    ORDER BY updated_at DESC, like_count DESC
                    LIMIT %s
                """
                params = [brands] + stock_params + [limit]
            else:
                sql = """
                    SELECT product_id
                    FROM products
                    WHERE brand = ANY(%s)
                    ORDER BY updated_at DESC, like_count DESC
                    LIMIT %s
                """
                params = [brands, limit]

        return self.fetch_val_list(sql, params)

    def get_adjacent_categories(
        self,
        user_categories: List[str],
        category_adjacency: Dict[str, List[str]],
        limit: int
    ) -> List[str]:
        """
        Get products in categories RELATED to user's preferences.
        Uses category_adjacency map to find related categories.
        """
        if not user_categories:
            return []

        # Find adjacent categories
        adjacent = []
        for cat in user_categories:
            adjacent.extend(category_adjacency.get(cat.lower(), []))

        if not adjacent:
            return []

        stock_filter, stock_params = self._build_stock_filter_clause()

        if stock_filter:
            sql = f"""
                SELECT product_id
                FROM products
                WHERE LOWER(category) = ANY(%s)
                  AND {stock_filter}
                ORDER BY like_count DESC, updated_at DESC
                LIMIT %s
            """
            params = [adjacent] + stock_params + [limit]
        else:
            sql = """
                SELECT product_id
                FROM products
                WHERE LOWER(category) = ANY(%s)
                ORDER BY like_count DESC, updated_at DESC
                LIMIT %s
            """
            params = [adjacent, limit]

        return self.fetch_val_list(sql, params)

    def get_trending_products(self, limit: int, hours: int = 48) -> List[str]:
        """
        Get products with recent engagement (trending).
        Trending = high like_count AND recent (past 24-48h)
        """
        stock_filter, stock_params = self._build_stock_filter_clause()

        if stock_filter:
            sql = f"""
                SELECT product_id
                FROM products
                WHERE parsed_at >= NOW() - INTERVAL '%s hours'
                  AND like_count > 5
                  AND {stock_filter}
                ORDER BY
                    like_count DESC,
                    parsed_at DESC
                LIMIT %s
            """
            params = [hours] + stock_params + [limit]
        else:
            sql = """
                SELECT product_id
                FROM products
                WHERE parsed_at >= NOW() - INTERVAL '%s hours'
                  AND like_count > 5
                ORDER BY
                    like_count DESC,
                    parsed_at DESC
                LIMIT %s
            """
            params = [hours, limit]

        return self.fetch_val_list(sql, params)

    def get_random_high_quality(self, limit: int) -> List[str]:
        """
        Get truly random products for discovery (serendipity).
        Only high-quality items (like_count >= 3).
        """
        stock_filter, stock_params = self._build_stock_filter_clause()

        if stock_filter:
            sql = f"""
                SELECT product_id
                FROM products
                WHERE like_count >= 3
                  AND {stock_filter}
                ORDER BY RANDOM()
                LIMIT %s
            """
            params = stock_params + [limit]
        else:
            sql = """
                SELECT product_id
                FROM products
                WHERE like_count >= 3
                ORDER BY RANDOM()
                LIMIT %s
            """
            params = [limit]

        return self.fetch_val_list(sql, params)

    def get_products_paginated(
        self,
        offset: int,
        limit: int,
        order_by: str = "updated_at"
    ) -> List[str]:
        """
        Get products with offset pagination for exhaustive traversal.
        """
        # Validate order_by to prevent SQL injection
        allowed_orders = ["updated_at", "created_at", "like_count", "price"]
        if order_by not in allowed_orders:
            order_by = "updated_at"

        stock_filter, stock_params = self._build_stock_filter_clause()

        if stock_filter:
            sql = f"""
                SELECT product_id
                FROM products
                WHERE {stock_filter}
                ORDER BY {order_by} DESC NULLS LAST, product_id
                LIMIT %s OFFSET %s
            """
            params = stock_params + [limit, offset]
        else:
            sql = f"""
                SELECT product_id
                FROM products
                ORDER BY {order_by} DESC NULLS LAST, product_id
                LIMIT %s OFFSET %s
            """
            params = [limit, offset]

        return self.fetch_val_list(sql, params)
