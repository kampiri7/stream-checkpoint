"""Elasticsearch backend for stream-checkpoint."""

import json
from datetime import datetime, timezone
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class ElasticsearchCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Elasticsearch.

    Args:
        client: An ``elasticsearch.Elasticsearch`` client instance.
        index: Name of the Elasticsearch index to use (default: ``"checkpoints"``).
    """

    def __init__(self, client, index: str = "checkpoints") -> None:
        self._client = client
        self._index = index
        self._ensure_index()

    def _ensure_index(self) -> None:
        """Create the index with appropriate mappings if it does not exist."""
        if not self._client.indices.exists(index=self._index):
            self._client.indices.create(
                index=self._index,
                body={
                    "mappings": {
                        "properties": {
                            "pipeline_id": {"type": "keyword"},
                            "offset": {"type": "keyword"},
                            "metadata": {"type": "object", "enabled": False},
                            "timestamp": {"type": "date"},
                        }
                    }
                },
            )

    def _doc_id(self, pipeline_id: str) -> str:
        return pipeline_id

    def save(self, checkpoint: Checkpoint) -> None:
        doc = checkpoint.to_dict()
        self._client.index(
            index=self._index,
            id=self._doc_id(checkpoint.pipeline_id),
            body=doc,
            refresh=True,
        )

    def load(self, pipeline_id: str) -> Optional[Checkpoint]:
        try:
            result = self._client.get(index=self._index, id=self._doc_id(pipeline_id))
            return Checkpoint.from_dict(result["_source"])
        except Exception:
            return None

    def delete(self, pipeline_id: str) -> None:
        try:
            self._client.delete(
                index=self._index,
                id=self._doc_id(pipeline_id),
                refresh=True,
            )
        except Exception:
            pass

    def list_checkpoints(self):
        result = self._client.search(
            index=self._index,
            body={"query": {"match_all": {}}, "size": 10000},
        )
        hits = result.get("hits", {}).get("hits", [])
        return [Checkpoint.from_dict(hit["_source"]) for hit in hits]
