from __future__ import annotations

from typing import Optional

try:
    from google.cloud import storage  # type: ignore
except Exception:  # pragma: no cover
    storage = None

from ..settings import Settings


class GCSClient:
    def __init__(self, settings: Settings):
        if not storage:
            raise RuntimeError("google-cloud-storage not installed")
        self._client = storage.Client(project=None)
        self._default_bucket = settings.gcs_bucket_composites

    def upload_bytes(
        self,
        bucket_name: Optional[str],
        blob_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Upload bytes to GCS and return the public URL. Bucket must have uniform public access."""
        bkt = bucket_name or self._default_bucket
        bucket = self._client.bucket(bkt)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(data, content_type=content_type)
        return f"https://storage.googleapis.com/{bkt}/{blob_name}"

    def get_blob_text(self, bucket_name: Optional[str], blob_name: str) -> Optional[str]:
        bucket = self._client.bucket(bucket_name or self._default_bucket)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            return None
        return blob.download_as_text()
