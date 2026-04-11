"""Redis checkpoint store with TTL (Time-To-Live) support."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class RedisTTLCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Redis with optional per-key TTL.

    Args:
        redis_client: A redis.Redis client (or compatible fake).
        ttl_seconds: Optional TTL in seconds applied on every save.
        key_prefix: Prefix for Redis keys.
    """

    def __init__(self, redis_client, ttl_seconds: Optional[int] = None, key_prefix: str = "ckpt"):
        self._client = redis_client
        self.ttl_seconds = ttl_seconds
        self.key_prefix = key_prefix

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self.key_prefix}:{pipeline_id}:{stream_id}"

    def _scan_prefix(self, prefix: str):
        """Return all keys matching prefix using SCAN."""
        cursor = 0
        keys = []
        while True:
            cursor, batch = self._client.scan(cursor, match=f"{prefix}*", count=100)
            keys.extend(batch)
            if cursor == 0:
                break
        return keys

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        value = json.dumps(checkpoint.to_dict())
        if self.ttl_seconds is not None:
            self._client.setex(key, self.ttl_seconds, value)
        else:
            self._client.set(key, value)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        raw = self._client.get(self._key(pipeline_id, stream_id))
        if raw is None:
            return None
        data = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
        return Checkpoint.from_dict(data)

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        self._client.delete(self._key(pipeline_id, stream_id))

    def list_checkpoints(self, pipeline_id: str):
        prefix = f"{self.key_prefix}:{pipeline_id}:"
        keys = self._scan_prefix(prefix)
        checkpoints = []
        for key in keys:
            raw = self._client.get(key)
            if raw is not None:
                data = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
                checkpoints.append(Checkpoint.from_dict(data))
        return checkpoints
