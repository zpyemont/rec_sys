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


def get_shown_set_fs(fs_client: FirestoreClient | None, user_id: str) -> Set[str]:
    if not fs_client:
        return set()
    try:
        col = fs_client.client.collection("user_feed_history").document(user_id).collection("shown")
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

