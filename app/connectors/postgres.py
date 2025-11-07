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
        if not prod_ids:
            return []
        # Use ANY with array param to avoid SQL injection if list is large
        sql = (
            "SELECT product_id as prod_id, title, price, images FROM products "
            "WHERE product_id = ANY(%s)"
        )
        rows = self.fetch_all(sql, (prod_ids,))
        out: List[Dict[str, Any]] = []
        for r in rows:
            images = r.get("images") or []
            image_url = images[0] if isinstance(images, list) and images else None
            out.append(
                {
                    "prod_id": str(r.get("prod_id")),
                    "title": r.get("title"),
                    "price": float(r.get("price")) if r.get("price") is not None else None,
                    "image_url": image_url,
                }
            )
        return out
