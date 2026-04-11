import json
from datetime import datetime
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class CouchbaseCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by Couchbase.

    Requires the ``couchbase`` package::

        pip install couchbase

    Parameters
    ----------
    bucket:
        An open Couchbase ``Bucket`` (or compatible) object.
    ttl:
        Optional time-to-live in seconds for stored documents.
        0 means no expiry (default).
    key_prefix:
        Prefix prepended to every document key.
    """

    def __init__(self, bucket, ttl: int = 0, key_prefix: str = "checkpoint:") -> None:
        self._bucket = bucket
        self._ttl = ttl
        self._prefix = key_prefix

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}{pipeline_id}:{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        data = json.dumps(checkpoint.to_dict())
        if self._ttl:
            self._bucket.upsert(key, data, ttl=self._ttl)
        else:
            self._bucket.upsert(key, data)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, stream_id)
        try:
            result = self._bucket.get(key)
            raw = result.value
            data = json.loads(raw) if isinstance(raw, str) else raw
            return Checkpoint.from_dict(data)
        except Exception:
            return None

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._key(pipeline_id, stream_id)
        try:
            self._bucket.remove(key)
        except Exception:
            pass

    def list_checkpoints(self, pipeline_id: str):
        raise NotImplementedError(
            "list_checkpoints requires a N1QL query; "
            "implement with a Couchbase cluster reference."
        )
