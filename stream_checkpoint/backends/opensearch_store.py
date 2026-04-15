import json
from datetime import datetime, timezone
from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class OpenSearchCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by OpenSearch."""

    def __init__(self, client, index: str = "checkpoints", pipeline: str = ""):
        """
        Args:
            client: An opensearch-py OpenSearch client instance.
            index: The OpenSearch index to use for storing checkpoints.
            pipeline: Optional ingest pipeline name.
        """
        self._client = client
        self._index = index
        self._pipeline = pipeline
        self._ensure_index()

    def _ensure_index(self) -> None:
        if not self._client.indices.exists(index=self._index):
            self._client.indices.create(
                index=self._index,
                body={
                    "mappings": {
                        "properties": {
                            "stream_id": {"type": "keyword"},
                            "partition": {"type": "keyword"},
                            "offset": {"type": "keyword"},
                            "metadata": {"type": "object", "enabled": False},
                            "updated_at": {"type": "date"},
                        }
                    }
                },
            )

    def _doc_id(self, stream_id: str, partition: str) -> str:
        return f"{stream_id}::{partition}"

    def save(self, checkpoint: Checkpoint) -> None:
        doc = checkpoint.to_dict()
        doc["updated_at"] = datetime.now(timezone.utc).isoformat()
        kwargs = dict(index=self._index, id=self._doc_id(checkpoint.stream_id, checkpoint.partition), body=doc)
        if self._pipeline:
            kwargs["pipeline"] = self._pipeline
        self._client.index(**kwargs)

    def load(self, stream_id: str, partition: str) -> Checkpoint | None:
        doc_id = self._doc_id(stream_id, partition)
        try:
            resp = self._client.get(index=self._index, id=doc_id)
            return Checkpoint.from_dict(resp["_source"])
        except Exception as exc:
            if "NotFoundError" in type(exc).__name__ or getattr(exc, "status_code", None) == 404:
                return None
            raise

    def delete(self, stream_id: str, partition: str) -> None:
        doc_id = self._doc_id(stream_id, partition)
        try:
            self._client.delete(index=self._index, id=doc_id)
        except Exception as exc:
            if "NotFoundError" in type(exc).__name__ or getattr(exc, "status_code", None) == 404:
                return
            raise

    def list_checkpoints(self, stream_id: str) -> list[Checkpoint]:
        resp = self._client.search(
            index=self._index,
            body={"query": {"term": {"stream_id": stream_id}}, "size": 1000},
        )
        return [Checkpoint.from_dict(hit["_source"]) for hit in resp["hits"]["hits"]]
