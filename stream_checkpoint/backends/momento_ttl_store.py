import json
from datetime import datetime
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class MomentoTTLCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Momento Cache with TTL support."""

    def __init__(self, client, cache_name: str, prefix: str = "checkpoint", ttl_seconds: int = 3600):
        """
        Args:
            client: Momento CacheClient instance.
            cache_name: Name of the Momento cache.
            prefix: Key prefix for namespacing.
            ttl_seconds: Time-to-live for each checkpoint in seconds.
        """
        self._client = client
        self._cache_name = cache_name
        self._prefix = prefix
        self._ttl_seconds = ttl_seconds

    def _key(self, stream_id: str, partition: str) -> str:
        return f"{self._prefix}:{stream_id}:{partition}"

    def save(self, checkpoint: Checkpoint) -> None:
        from datetime import timedelta
        key = self._key(checkpoint.stream_id, checkpoint.partition)
        payload = json.dumps(checkpoint.to_dict())
        self._client.set(
            self._cache_name,
            key,
            payload,
            timedelta(seconds=self._ttl_seconds),
        )

    def load(self, stream_id: str, partition: str) -> Optional[Checkpoint]:
        key = self._key(stream_id, partition)
        response = self._client.get(self._cache_name, key)
        hit_class = getattr(self._client, "_HitResponse", None)
        # duck-type: check for value attribute
        value = getattr(response, "value_string", None)
        if value is None:
            return None
        data = json.loads(value)
        return Checkpoint.from_dict(data)

    def delete(self, stream_id: str, partition: str) -> None:
        key = self._key(stream_id, partition)
        self._client.delete(self._cache_name, key)

    def list_checkpoints(self, stream_id: str):
        raise NotImplementedError("Momento does not support listing checkpoints by stream_id")
