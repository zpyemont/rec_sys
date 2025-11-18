from __future__ import annotations

from typing import Iterable, Set

try:
    from google.cloud import firestore  # type: ignore
except Exception:  # pragma: no cover
    firestore = None

from ..settings import Settings


class FirestoreClient:
    def __init__(self, project: str | None = None):
        if not firestore:
            raise RuntimeError("google-cloud-firestore not installed")
        self._client = firestore.Client(project=project) if project else firestore.Client()

    @classmethod
    def from_settings(cls, settings: Settings) -> "FirestoreClient":
        return cls(project=settings.bq_project)

    @property
    def client(self):
        return self._client


def get_firestore_client_safe(settings: Settings):
    if not firestore:
        return None
    try:
        return FirestoreClient.from_settings(settings)
    except Exception:
        return None


def get_shown_set_fs(
    fs_client: FirestoreClient | None,
    user_id: str,
    ttl_days: int = 30
) -> Set[str]:
    """
    Get shown items, optionally filtering by TTL.
    Items shown more than ttl_days ago are excluded.
    """
    if not fs_client:
        return set()
    try:
        col = fs_client.client.collection("user_feed_history").document(user_id).collection("shown")

        if ttl_days and ttl_days > 0:
            # Calculate cutoff timestamp
            from datetime import datetime, timedelta
            cutoff = datetime.utcnow() - timedelta(days=ttl_days)

            # Query only recent items
            docs = col.where("shown_at", ">=", cutoff).stream()
        else:
            # No TTL, get all items
            docs = col.stream()

        return {d.id for d in docs}
    except Exception:
        return set()


def add_shown_items_fs(fs_client: FirestoreClient | None, user_id: str, prod_ids: Iterable[str]) -> None:
    if not fs_client:
        return
    try:
        if not prod_ids:
            return
        batch = fs_client.client.batch()
        col = fs_client.client.collection("user_feed_history").document(user_id).collection("shown")
        for pid in prod_ids:
            doc_ref = col.document(str(pid))
            batch.set(doc_ref, {"shown_at": firestore.SERVER_TIMESTAMP}, merge=True)
        batch.commit()
    except Exception:
        return


def cleanup_old_shown_items(
    fs_client: FirestoreClient | None,
    user_id: str,
    days_old: int = 30
) -> int:
    """
    Delete shown items older than days_old.
    Returns number of items deleted.

    Call this periodically to prevent shown_set from growing unbounded.
    """
    if not fs_client:
        return 0

    try:
        from datetime import datetime, timedelta

        cutoff = datetime.utcnow() - timedelta(days=days_old)
        col = fs_client.client.collection("user_feed_history").document(user_id).collection("shown")

        # Query old items
        old_docs = col.where("shown_at", "<", cutoff).stream()

        # Delete in batches (Firestore batch limit is 500)
        batch = fs_client.client.batch()
        count = 0
        for doc in old_docs:
            batch.delete(doc.reference)
            count += 1

            # Commit every 500 deletes
            if count % 500 == 0:
                batch.commit()
                batch = fs_client.client.batch()

        # Commit remaining
        if count % 500 != 0:
            batch.commit()

        return count
    except Exception:
        return 0

