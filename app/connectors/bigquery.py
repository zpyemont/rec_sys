from __future__ import annotations

from typing import Any, Dict, List

try:
    from google.cloud import bigquery  # type: ignore
except Exception:  # pragma: no cover
    bigquery = None

from ..settings import Settings


class BigQueryClient:
    def __init__(self, project: str | None):
        if not bigquery:
            raise RuntimeError("google-cloud-bigquery not installed")
        self._client = bigquery.Client(project=project) if project else bigquery.Client()

    @classmethod
    def from_settings(cls, settings: Settings) -> "BigQueryClient":
        return cls(project=settings.bq_project)

    def get_product_metadata_for_ids(self, dataset: str, table: str, prod_ids: List[str]) -> List[Dict[str, Any]]:
        if not prod_ids:
            return []
        ids_param = ",".join([f"'{str(pid)}'" for pid in prod_ids])
        query = f"""
        SELECT
          CAST(product_id AS STRING) AS prod_id,
          title,
          price,
          images
        FROM `{dataset}.{table}`
        WHERE product_id IN ({ids_param})
        """
        job = self._client.query(query)
        rows = list(job)
        out: List[Dict[str, Any]] = []
        for r in rows:
            images = r.get("images") if hasattr(r, "get") else getattr(r, "images", None)
            if images is None and hasattr(r, "images"):
                images = r.images
            if isinstance(images, list) and images:
                image_url = images[0]
            else:
                image_url = None
            prod_id = r.get("prod_id") if hasattr(r, "get") else getattr(r, "prod_id", None)
            title = r.get("title") if hasattr(r, "get") else getattr(r, "title", None)
            price_val = r.get("price") if hasattr(r, "get") else getattr(r, "price", None)
            out.append(
                {
                    "prod_id": str(prod_id) if prod_id is not None else None,
                    "title": title,
                    "price": float(price_val) if price_val is not None else None,
                    "image_url": image_url,
                }
            )
        return out
