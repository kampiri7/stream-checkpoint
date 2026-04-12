import json
from datetime import datetime
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class MomentoCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Momento Cache.

    Args:
        client: A Momento ``CacheClient`` instance.
        cache_name: Name of the Momento cache to use.
        ttl: Optional time-to-live in seconds for each checkpoint entry.
             Defaults to ``None`` (no expiry — Momento will use the cache
             default TTL set at creation time).
        prefix: Key prefix applied to every checkpoint key.
    """

    def __init__(
        self,
        client,
        cache_name: str,
        ttl: Optional[int] = None,
        prefix: str = "checkpoint:",
    ) -> None:
        self._client = client
        self._cache_name = cache_name
        self._ttl = ttl
        self._prefix = prefix

    def _key(self, stream_id: str, partition: str) -> str:
        return f"{self._prefix}{stream_id}:{partition}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.stream_id, checkpoint.partition)
        payload = json.dumps(checkpoint.to_dict())
        if self._ttl is not None:
            self._client.set(self._cache_name, key, payload, ttl_seconds=self._ttl)
        else:
            self._client.set(self._cache_name, key, payload)

    def load(self, stream_id: str, partition: str) -> Optional[Checkpoint]:
        key = self._key(stream_id, partition)
        response = self._client.get(self._cache_name, key)
        value = getattr(response, "value_string", None)
        if value is None:
            return None
        return Checkpoint.from_dict(json.loads(value))

    def delete(self, stream_id: str, partition: str) -> None:
        key = self._key(stream_id, partition)
        self._client.delete(self._cache_name, key)

    def list_checkpoints(self, stream_id: str) -> list:
        # Momento does not expose a native key-scan API; callers should
        # maintain their own partition registry when enumeration is needed.
        raise NotImplementedError(
            "MomentoCheckpointStore does not support list_checkpoints. "
            "Maintain a separate partition registry for enumeration."
        )
