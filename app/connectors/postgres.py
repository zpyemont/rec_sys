"""
PostgreSQL client for rec_sys (v2 - normalized schema).

Uses the new schema structure with JOINs across:
- catalog.products - Core product data
- catalog.product_pricing - Pricing, availability, like_count
- catalog.product_images - Product images (aggregated)
- embeddings.product_vectors - Image embeddings
- health.product_status - Health/staleness (for is_active)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence
import contextlib

import psycopg2
import psycopg2.extras
import psycopg2.pool

try:
    import asyncpg
except ImportError:
    asyncpg = None

from ..settings import Settings

logger = logging.getLogger(__name__)


class AsyncPostgresClient:
    """Async PostgreSQL client for the new normalized schema."""

    _pool = None
    _pool_dsn = None

    def __init__(self, dsn: str):
        if asyncpg is None:
            raise RuntimeError("asyncpg not available")
        self._dsn = dsn

    @classmethod
    def from_settings(cls, settings: Settings) -> "AsyncPostgresClient":
        from urllib.parse import quote

        if settings.postgres_dsn:
            dsn = cls._convert_dsn_to_url(settings.postgres_dsn)
        else:
            host = settings.pg_host or "localhost"
            port = settings.pg_port or 5432
            user = settings.pg_user or "postgres"
            password = quote(settings.pg_password or "", safe="")
            database = settings.pg_database or "looksy"
            dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        return cls(dsn)

    @staticmethod
    def _convert_dsn_to_url(dsn: str) -> str:
        from urllib.parse import quote

        if dsn.startswith("postgresql://") or dsn.startswith("postgres://"):
            return dsn

        parts = {}
        for item in dsn.split():
            if "=" in item:
                key, value = item.split("=", 1)
                parts[key] = value

        host = parts.get("host", "localhost")
        port = parts.get("port", "5432")
        user = parts.get("user", "postgres")
        password = quote(parts.get("password", ""), safe="")
        dbname = parts.get("dbname", "looksy")

        return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    async def get_pool(self):
        if AsyncPostgresClient._pool is None or AsyncPostgresClient._pool_dsn != self._dsn:
            if AsyncPostgresClient._pool is not None:
                await AsyncPostgresClient._pool.close()
            AsyncPostgresClient._pool = await asyncpg.create_pool(
                self._dsn,
                min_size=1,
                max_size=5,
            )
            AsyncPostgresClient._pool_dsn = self._dsn
            logger.info("AsyncPostgresClient pool created")
        return AsyncPostgresClient._pool

    async def fetch_all(self, sql: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *(params or []))
            return [dict(row) for row in rows]

    async def fetch_val_list(
        self,
        sql: str,
        params: Optional[List] = None,
        col: str = "product_id"
    ) -> List[str]:
        rows = await self.fetch_all(sql, params)
        return [str(row[col]) for row in rows if row.get(col) is not None]

    async def fetch_one(self, sql: str, params: Optional[List] = None) -> Optional[Dict[str, Any]]:
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(sql, *(params or []))
            return dict(row) if row else None

    async def execute(self, sql: str, params: Optional[List] = None) -> str:
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            return await conn.execute(sql, *(params or []))

    async def get_product_metadata_for_ids(self, prod_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Fetch full product metadata for given product IDs."""
        if not prod_ids:
            return {}

        rows = await self.fetch_all("""
            SELECT
                p.product_id,
                p.title,
                p.description,
                p.brand,
                p.category,
                p.subcategory,
                p.url,
                p.created_at,
                pr.price,
                pr.compare_at_price,
                pr.currency,
                pr.availability,
                pr.like_count,
                ARRAY_AGG(i.image_url ORDER BY i.position) FILTER (WHERE i.image_url IS NOT NULL) as images,
                ARRAY_AGG(i.has_text_overlay ORDER BY i.position) FILTER (WHERE i.image_url IS NOT NULL) as image_has_text
            FROM catalog.products p
            JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
            LEFT JOIN catalog.product_images i ON p.product_id = i.product_id
            WHERE p.product_id = ANY($1) AND pr.is_active = true
            GROUP BY p.product_id, p.title, p.description, p.brand, p.category,
                     p.subcategory, p.url, p.created_at, pr.price, pr.compare_at_price,
                     pr.currency, pr.availability, pr.like_count
        """, [prod_ids])

        return {row['product_id']: row for row in rows}


class PostgresClient:
    """Synchronous PostgreSQL client for the new normalized schema."""

    _pool = None
    _pool_dsn = None

    def __init__(self, dsn: str, settings: Optional[Settings] = None):
        self._dsn = dsn
        self._settings = settings
        self._init_pool()

    def _init_pool(self):
        if PostgresClient._pool is None or PostgresClient._pool_dsn != self._dsn:
            if PostgresClient._pool is not None:
                PostgresClient._pool.closeall()
            PostgresClient._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=5,
                dsn=self._dsn
            )
            PostgresClient._pool_dsn = self._dsn
            logger.info("PostgresClient connection pool created")

    @classmethod
    def from_settings(cls, settings: Settings) -> "PostgresClient":
        if settings.postgres_dsn:
            dsn = settings.postgres_dsn
        else:
            host = settings.pg_host or "localhost"
            port = settings.pg_port or 5432
            user = settings.pg_user or "postgres"
            password = settings.pg_password or ""
            database = settings.pg_database or "looksy"
            dsn = f"host={host} port={port} user={user} password={password} dbname={database}"
        return cls(dsn, settings)

    @contextlib.contextmanager
    def _get_conn(self):
        conn = PostgresClient._pool.getconn()
        try:
            yield conn
        finally:
            try:
                PostgresClient._pool.putconn(conn)
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
        """Build SQL WHERE clause to filter out-of-stock and inactive products."""
        clauses = ["pr.is_active = true"]
        params = []

        if self._settings and self._settings.filter_out_of_stock:
            excluded_values = self._settings.excluded_availability_values
            if excluded_values:
                placeholders = ", ".join(["%s"] * len(excluded_values))
                clauses.append(f"(pr.availability IS NULL OR LOWER(pr.availability) NOT IN ({placeholders}))")
                params.extend([v.lower() for v in excluded_values])

        return (" AND ".join(clauses), params)

    def get_recent_products(self, hours: int, limit: int) -> List[str]:
        stock_filter, stock_params = self._build_stock_filter_clause()

        sql = f"""
            SELECT p.product_id
            FROM catalog.products p
            JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
            WHERE p.parsed_at >= NOW() - INTERVAL '%s hours'
              AND {stock_filter}
            ORDER BY p.parsed_at DESC NULLS LAST
            LIMIT %s
        """
        params = [hours] + stock_params + [limit]
        return self.fetch_val_list(sql, params)

    def get_recent_products_diverse(self, hours: int, limit: int, max_per_brand: int = 5) -> List[str]:
        stock_filter, stock_params = self._build_stock_filter_clause()

        sql = f"""
            WITH ranked AS (
                SELECT p.product_id, p.brand,
                       ROW_NUMBER() OVER (PARTITION BY p.brand ORDER BY p.parsed_at DESC) as rn
                FROM catalog.products p
                JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
                WHERE p.parsed_at >= NOW() - INTERVAL '%s hours'
                  AND {stock_filter}
            )
            SELECT product_id FROM ranked
            WHERE rn <= %s
            ORDER BY rn, brand
            LIMIT %s
        """
        params = [hours] + stock_params + [max_per_brand, limit]
        return self.fetch_val_list(sql, params)

    def get_popular_products(self, limit: int) -> List[str]:
        stock_filter, stock_params = self._build_stock_filter_clause()

        sql = f"""
            SELECT p.product_id
            FROM catalog.products p
            JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
            WHERE {stock_filter}
            ORDER BY pr.like_count DESC, pr.updated_at DESC NULLS LAST
            LIMIT %s
        """
        params = stock_params + [limit]
        return self.fetch_val_list(sql, params)

    def get_popular_products_diverse(self, limit: int, max_per_brand: int = 5) -> List[str]:
        stock_filter, stock_params = self._build_stock_filter_clause()

        sql = f"""
            WITH ranked AS (
                SELECT p.product_id, p.brand,
                       ROW_NUMBER() OVER (PARTITION BY p.brand ORDER BY pr.like_count DESC, pr.updated_at DESC) as rn
                FROM catalog.products p
                JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
                WHERE {stock_filter}
            )
            SELECT product_id FROM ranked
            WHERE rn <= %s
            ORDER BY rn, brand
            LIMIT %s
        """
        params = stock_params + [max_per_brand, limit]
        return self.fetch_val_list(sql, params)

    def get_by_brand_or_vendor(self, cat: str, limit: int) -> List[str]:
        stock_filter, stock_params = self._build_stock_filter_clause()

        sql = f"""
            SELECT p.product_id
            FROM catalog.products p
            JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
            WHERE (LOWER(p.brand) = LOWER(%s) OR LOWER(p.vendor) = LOWER(%s))
              AND {stock_filter}
            ORDER BY pr.updated_at DESC NULLS LAST
            LIMIT %s
        """
        params = [cat, cat] + stock_params + [limit]
        return self.fetch_val_list(sql, params)

    def get_product_metadata_for_ids(self, prod_ids: List[str]) -> List[Dict[str, Any]]:
        """Fetch full product metadata for given product IDs."""
        if not prod_ids:
            return []

        sql = """
            SELECT
                p.product_id,
                p.title,
                p.description,
                p.brand,
                p.category,
                p.subcategory,
                p.url,
                p.created_at,
                pr.price,
                pr.compare_at_price,
                pr.currency,
                pr.availability,
                pr.like_count,
                ARRAY_AGG(i.image_url ORDER BY i.position) FILTER (WHERE i.image_url IS NOT NULL) as images,
                ARRAY_AGG(i.has_text_overlay ORDER BY i.position) FILTER (WHERE i.image_url IS NOT NULL) as image_has_text
            FROM catalog.products p
            JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
            LEFT JOIN catalog.product_images i ON p.product_id = i.product_id
            WHERE p.product_id = ANY(%s) AND pr.is_active = true
            GROUP BY p.product_id, p.title, p.description, p.brand, p.category,
                     p.subcategory, p.url, p.created_at, pr.price, pr.compare_at_price,
                     pr.currency, pr.availability, pr.like_count
        """
        return self.fetch_all(sql, (prod_ids,))

    def increment_like_count(self, product_id: str) -> int:
        """Increment like count for a product."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE catalog.product_pricing
                    SET like_count = like_count + 1,
                        updated_at = NOW()
                    WHERE product_id = %s
                    RETURNING like_count
                """, (product_id,))
                result = cur.fetchone()
                conn.commit()
                return result[0] if result else 0

    def decrement_like_count(self, product_id: str) -> int:
        """Decrement like count for a product (min 0)."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE catalog.product_pricing
                    SET like_count = GREATEST(like_count - 1, 0),
                        updated_at = NOW()
                    WHERE product_id = %s
                    RETURNING like_count
                """, (product_id,))
                result = cur.fetchone()
                conn.commit()
                return result[0] if result else 0

    def get_total_product_count(self) -> int:
        """Get total count of active products."""
        stock_filter, stock_params = self._build_stock_filter_clause()

        sql = f"""
            SELECT COUNT(*) as count
            FROM catalog.products p
            JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
            WHERE {stock_filter}
        """
        rows = self.fetch_all(sql, stock_params)
        return rows[0]['count'] if rows else 0

    def get_candidates_from_categories(
        self,
        categories: List[str],
        price_range: Optional[Dict],
        limit: int
    ) -> List[str]:
        """Get products in specific categories."""
        if not categories:
            return []

        stock_filter, stock_params = self._build_stock_filter_clause()
        categories_lower = [cat.lower() for cat in categories]

        if price_range and price_range.get('min') and price_range.get('max'):
            sql = f"""
                SELECT p.product_id
                FROM catalog.products p
                JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
                WHERE (p.subcategory = ANY(%s) OR LOWER(p.category) = ANY(%s))
                  AND pr.price BETWEEN %s AND %s
                  AND {stock_filter}
                ORDER BY pr.like_count DESC, pr.updated_at DESC
                LIMIT %s
            """
            params = [categories, categories_lower, price_range['min'], price_range['max']] + stock_params + [limit]
        else:
            sql = f"""
                SELECT p.product_id
                FROM catalog.products p
                JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
                WHERE (p.subcategory = ANY(%s) OR LOWER(p.category) = ANY(%s))
                  AND {stock_filter}
                ORDER BY pr.like_count DESC, pr.updated_at DESC
                LIMIT %s
            """
            params = [categories, categories_lower] + stock_params + [limit]

        return self.fetch_val_list(sql, params)

    def get_candidates_from_categories_diverse(
        self,
        categories: List[str],
        price_range: Optional[Dict],
        limit: int,
        max_per_brand: int = 20
    ) -> List[str]:
        """Get products in specific categories with brand diversity."""
        if not categories:
            return []

        stock_filter, stock_params = self._build_stock_filter_clause()
        categories_lower = [cat.lower() for cat in categories]

        if price_range and price_range.get('min') and price_range.get('max'):
            sql = f"""
                WITH ranked AS (
                    SELECT p.product_id, p.brand,
                           ROW_NUMBER() OVER (PARTITION BY p.brand ORDER BY pr.like_count DESC) as rn
                    FROM catalog.products p
                    JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
                    WHERE (p.subcategory = ANY(%s) OR LOWER(p.category) = ANY(%s))
                      AND pr.price BETWEEN %s AND %s
                      AND {stock_filter}
                )
                SELECT product_id FROM ranked
                WHERE rn <= %s
                ORDER BY rn, brand
                LIMIT %s
            """
            params = [categories, categories_lower, price_range['min'], price_range['max']] + stock_params + [max_per_brand, limit]
        else:
            sql = f"""
                WITH ranked AS (
                    SELECT p.product_id, p.brand,
                           ROW_NUMBER() OVER (PARTITION BY p.brand ORDER BY pr.like_count DESC) as rn
                    FROM catalog.products p
                    JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
                    WHERE (p.subcategory = ANY(%s) OR LOWER(p.category) = ANY(%s))
                      AND {stock_filter}
                )
                SELECT product_id FROM ranked
                WHERE rn <= %s
                ORDER BY rn, brand
                LIMIT %s
            """
            params = [categories, categories_lower] + stock_params + [max_per_brand, limit]

        return self.fetch_val_list(sql, params)

    def get_candidates_from_brands(
        self,
        brands: List[str],
        price_range: Optional[Dict],
        limit: int
    ) -> List[str]:
        """Get products from specific brands."""
        if not brands:
            return []

        stock_filter, stock_params = self._build_stock_filter_clause()
        brands_lower = [b.lower() for b in brands]

        if price_range and price_range.get('min') and price_range.get('max'):
            sql = f"""
                SELECT p.product_id
                FROM catalog.products p
                JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
                WHERE LOWER(p.brand) = ANY(%s)
                  AND pr.price BETWEEN %s AND %s
                  AND {stock_filter}
                ORDER BY pr.like_count DESC
                LIMIT %s
            """
            params = [brands_lower, price_range['min'], price_range['max']] + stock_params + [limit]
        else:
            sql = f"""
                SELECT p.product_id
                FROM catalog.products p
                JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
                WHERE LOWER(p.brand) = ANY(%s)
                  AND {stock_filter}
                ORDER BY pr.like_count DESC
                LIMIT %s
            """
            params = [brands_lower] + stock_params + [limit]

        return self.fetch_val_list(sql, params)

    def get_trending_products(self, hours: int = 48, limit: int = 100) -> List[str]:
        """Get trending products (high likes + recent)."""
        stock_filter, stock_params = self._build_stock_filter_clause()

        sql = f"""
            SELECT p.product_id
            FROM catalog.products p
            JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
            WHERE p.parsed_at >= NOW() - INTERVAL '%s hours'
              AND pr.like_count >= 1
              AND {stock_filter}
            ORDER BY pr.like_count DESC, p.parsed_at DESC
            LIMIT %s
        """
        params = [hours] + stock_params + [limit]
        return self.fetch_val_list(sql, params)

    def get_random_high_quality(self, min_likes: int = 3, limit: int = 50) -> List[str]:
        """Get random high-quality products for serendipity."""
        stock_filter, stock_params = self._build_stock_filter_clause()

        sql = f"""
            SELECT p.product_id
            FROM catalog.products p
            JOIN catalog.product_pricing pr ON p.product_id = pr.product_id
            WHERE pr.like_count >= %s
              AND {stock_filter}
            ORDER BY RANDOM()
            LIMIT %s
        """
        params = [min_likes] + stock_params + [limit]
        return self.fetch_val_list(sql, params)

    def record_like(self, user_id: str, product_id: str) -> bool:
        """Record a like in engagement.likes table."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("""
                        INSERT INTO engagement.likes (user_id, product_id)
                        VALUES (%s, %s)
                        ON CONFLICT (user_id, product_id) DO NOTHING
                    """, (user_id, product_id))
                    conn.commit()
                    return True
                except Exception as e:
                    logger.error(f"Failed to record like: {e}")
                    conn.rollback()
                    return False

    def remove_like(self, user_id: str, product_id: str) -> bool:
        """Remove a like from engagement.likes table."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("""
                        DELETE FROM engagement.likes
                        WHERE user_id = %s AND product_id = %s
                    """, (user_id, product_id))
                    conn.commit()
                    return True
                except Exception as e:
                    logger.error(f"Failed to remove like: {e}")
                    conn.rollback()
                    return False

    def get_user_likes(self, user_id: str, limit: int = 100) -> List[str]:
        """Get product IDs liked by a user."""
        sql = """
            SELECT product_id FROM engagement.likes
            WHERE user_id = %s
            ORDER BY created_at DESC
            LIMIT %s
        """
        return self.fetch_val_list(sql, (user_id, limit))

    def get_freshness_timestamps(self, prod_ids: List[str]) -> Dict[str, float]:
        """
        Get freshness timestamps for product IDs, returning hours since created_at.

        Returns:
            Dict mapping product_id to hours since creation (lower = fresher)
        """
        if not prod_ids:
            return {}

        sql = """
            SELECT
                p.product_id,
                EXTRACT(EPOCH FROM (NOW() - COALESCE(p.created_at, p.parsed_at))) / 3600.0 as hours_old
            FROM catalog.products p
            WHERE p.product_id = ANY(%s)
        """
        rows = self.fetch_all(sql, (prod_ids,))
        return {row['product_id']: float(row['hours_old']) if row['hours_old'] else 0.0 for row in rows}
