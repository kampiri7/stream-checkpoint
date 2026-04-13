"""Qdrant checkpoint store backend."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class QdrantCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Qdrant key-value (payload) storage.

    Uses Qdrant's REST/gRPC client to store checkpoints as point payloads
    in a dedicated collection.  Each checkpoint occupies a single point
    whose ``id`` is derived from ``pipeline_id`` and ``stream_id``.
    """

    def __init__(
        self,
        client,
        collection_name: str = "stream_checkpoints",
        prefix: str = "ckpt",
    ) -> None:
        self._client = client
        self._collection = collection_name
        self._prefix = prefix
        self._ensure_collection()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_collection(self) -> None:
        """Create the collection if it does not already exist."""
        existing = [
            c.name for c in self._client.get_collections().collections
        ]
        if self._collection not in existing:
            from qdrant_client.models import VectorParams, Distance

            self._client.create_collection(
                collection_name=self._collection,
                vectors_config=VectorParams(size=1, distance=Distance.DOT),
            )

    def _point_id(self, pipeline_id: str, stream_id: str) -> str:
        """Return a stable string point id for the given ids."""
        return f"{self._prefix}:{pipeline_id}:{stream_id}"

    # ------------------------------------------------------------------
    # BaseCheckpointStore interface
    # ------------------------------------------------------------------

    def save(self, checkpoint: Checkpoint) -> None:
        from qdrant_client.models import PointStruct

        point_id = self._point_id(checkpoint.pipeline_id, checkpoint.stream_id)
        payload = checkpoint.to_dict()
        self._client.upsert(
            collection_name=self._collection,
            points=[
                PointStruct(
                    id=abs(hash(point_id)) % (2 ** 63),
                    vector=[0.0],
                    payload={"_ckpt_key": point_id, **payload},
                )
            ],
        )

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        point_id = self._point_id(pipeline_id, stream_id)
        results = self._client.scroll(
            collection_name=self._collection,
            scroll_filter={
                "must": [{"key": "_ckpt_key", "match": {"value": point_id}}]
            },
            limit=1,
            with_payload=True,
        )
        points, _ = results
        if not points:
            return None
        payload = dict(points[0].payload)
        payload.pop("_ckpt_key", None)
        return Checkpoint.from_dict(payload)

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        point_id = self._point_id(pipeline_id, stream_id)
        self._client.delete(
            collection_name=self._collection,
            points_selector={
                "filter": {
                    "must": [
                        {"key": "_ckpt_key", "match": {"value": point_id}}
                    ]
                }
            },
        )

    def list_checkpoints(self, pipeline_id: str):
        prefix = f"{self._prefix}:{pipeline_id}:"
        results, _ = self._client.scroll(
            collection_name=self._collection,
            scroll_filter={
                "must": [
                    {"key": "_ckpt_key", "match": {"text": prefix}}
                ]
            },
            limit=1000,
            with_payload=True,
        )
        checkpoints = []
        for point in results:
            payload = dict(point.payload)
            payload.pop("_ckpt_key", None)
            checkpoints.append(Checkpoint.from_dict(payload))
        return checkpoints
