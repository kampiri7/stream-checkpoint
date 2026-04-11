import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class MemcachedCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by Memcached.

    Args:
        client: A pymemcache-compatible client instance.
        prefix: Key prefix to namespace checkpoints.
        ttl: Time-to-live in seconds (0 = no expiry).
    """

    def __init__(self, client, prefix: str = "checkpoint:", ttl: int = 0):
        self._client = client
        self._prefix = prefix
        self._ttl = ttl

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}{pipeline_id}:{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        value = json.dumps(checkpoint.to_dict()).encode("utf-8")
        self._client.set(key, value, expire=self._ttl)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, stream_id)
        value = self._client.get(key)
        if value is None:
            return None
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        return Checkpoint.from_dict(json.loads(value))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._key(pipeline_id, stream_id)
        self._client.delete(key)

    def list_checkpoints(self, pipeline_id: str):
        raise NotImplementedError(
            "Memcached does not support key enumeration. "
            "Use a different backend if listing is required."
        )
