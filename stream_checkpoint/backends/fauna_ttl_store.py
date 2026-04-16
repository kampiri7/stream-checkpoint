import json
import time
from stream_checkpoint.base import Checkpoint, BaseCheckpointStore


class FaunaTTLCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by FaunaDB with TTL support via expires_at field."""

    def __init__(self, client, collection: str = "checkpoints", ttl_seconds: int = None):
        self._client = client
        self._collection = collection
        self._ttl_seconds = ttl_seconds

    def _record_id(self, pipeline_id: str, partition: str) -> str:
        return f"{pipeline_id}::{partition}"

    def save(self, checkpoint: Checkpoint) -> None:
        q = self._client.query
        data = checkpoint.to_dict()
        if self._ttl_seconds is not None:
            data["expires_at"] = time.time() + self._ttl_seconds
        record_id = self._record_id(checkpoint.pipeline_id, checkpoint.partition)
        existing = self._query(record_id)
        if existing:
            q.update(q.ref(q.collection(self._collection), existing["ref"]), {"data": data})
        else:
            q.create(q.collection(self._collection), {"data": {**data, "record_id": record_id}})

    def load(self, pipeline_id: str, partition: str):
        record_id = self._record_id(pipeline_id, partition)
        doc = self._query(record_id)
        if doc is None:
            return None
        data = doc["data"]
        if "expires_at" in data and time.time() > data["expires_at"]:
            self.delete(pipeline_id, partition)
            return None
        return Checkpoint.from_dict(data)

    def delete(self, pipeline_id: str, partition: str) -> None:
        record_id = self._record_id(pipeline_id, partition)
        doc = self._query(record_id)
        if doc:
            q = self._client.query
            q.delete(q.ref(q.collection(self._collection), doc["ref"]))

    def list_checkpoints(self, pipeline_id: str):
        q = self._client.query
        results = q.paginate(q.match(q.index(f"{self._collection}_by_pipeline"), pipeline_id))
        now = time.time()
        checkpoints = []
        for item in results.get("data", []):
            data = item.get("data", {})
            if "expires_at" in data and now > data["expires_at"]:
                continue
            checkpoints.append(Checkpoint.from_dict(data))
        return checkpoints

    def _query(self, record_id: str):
        return self._client.find_one(self._collection, record_id)
