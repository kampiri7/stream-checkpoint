"""Firestore checkpoint store with TTL support."""

import json
from datetime import datetime, timezone, timedelta
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class FirestoreTTLCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Google Firestore with optional TTL.

    Args:
        client: A ``google.cloud.firestore.Client`` instance.
        collection: Firestore collection name (default: ``"checkpoints"``).
        ttl_seconds: Optional TTL in seconds. When set, an ``expires_at``
            field is written alongside the document so a Firestore TTL
            policy can expire stale records automatically.
    """

    def __init__(self, client, collection: str = "checkpoints", ttl_seconds: Optional[int] = None):
        self._client = client
        self._collection = collection
        self._ttl_seconds = ttl_seconds

    def _doc_id(self, pipeline_id: str, stream_id: str) -> str:
        return f"{pipeline_id}__{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        doc_id = self._doc_id(checkpoint.pipeline_id, checkpoint.stream_id)
        data = checkpoint.to_dict()
        data["payload"] = json.dumps(data.get("metadata", {}))
        if self._ttl_seconds is not None:
            data["expires_at"] = datetime.now(tz=timezone.utc) + timedelta(seconds=self._ttl_seconds)
        self._client.collection(self._collection).document(doc_id).set(data)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        doc_id = self._doc_id(pipeline_id, stream_id)
        snapshot = self._client.collection(self._collection).document(doc_id).get()
        if not snapshot.exists:
            return None
        data = snapshot.to_dict()
        data.pop("expires_at", None)
        data.pop("payload", None)
        return Checkpoint.from_dict(data)

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        doc_id = self._doc_id(pipeline_id, stream_id)
        self._client.collection(self._collection).document(doc_id).delete()

    def list_checkpoints(self, pipeline_id: str):
        docs = self._client.collection(self._collection).where(
            "pipeline_id", "==", pipeline_id
        ).stream()
        results = []
        for doc in docs:
            data = doc.to_dict()
            data.pop("expires_at", None)
            data.pop("payload", None)
            results.append(Checkpoint.from_dict(data))
        return results
