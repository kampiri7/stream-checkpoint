"""KeyDB checkpoint store backend.

KeyDB is a high-performance, Redis-compatible database. This backend
uses the same interface as the Redis store but targets a KeyDB server.
"""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class KeyDBCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by KeyDB.

    Args:
        client: A KeyDB client instance (e.g. from the ``keydb`` or
            ``redis`` package — KeyDB is wire-compatible with Redis).
        prefix: Optional key prefix used to namespace checkpoints.
        ttl: Optional time-to-live in seconds. When set, saved
            checkpoints expire automatically after *ttl* seconds.
    """

    def __init__(self, client, prefix: str = "checkpoint", ttl: Optional[int] = None):
        self._client = client
        self._prefix = prefix
        self._ttl = ttl

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}:{pipeline_id}:{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        value = json.dumps(checkpoint.to_dict())
        if self._ttl is not None:
            self._client.setex(key, self._ttl, value)
        else:
            self._client.set(key, value)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, stream_id)
        raw = self._client.get(key)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode()
        return Checkpoint.from_dict(json.loads(raw))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._key(pipeline_id, stream_id)
        self._client.delete(key)

    def list_checkpoints(self, pipeline_id: str):
        pattern = self._key(pipeline_id, "*")
        keys = self._client.keys(pattern)
        checkpoints = []
        for key in keys:
            raw = self._client.get(key)
            if raw is not None:
                if isinstance(raw, bytes):
                    raw = raw.decode()
                checkpoints.append(Checkpoint.from_dict(json.loads(raw)))
        return checkpoints
