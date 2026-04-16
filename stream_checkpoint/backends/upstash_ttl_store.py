import json
from datetime import datetime
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class UpstashTTLCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Upstash Redis with TTL support."""

    def __init__(self, client, prefix: str = "checkpoint", ttl: int = 3600):
        """
        Args:
            client: Upstash Redis client instance.
            prefix: Key prefix for namespacing checkpoints.
            ttl: Time-to-live in seconds for each checkpoint (default 3600).
        """
        self._client = client
        self._prefix = prefix
        self._ttl = ttl

    def _key(self, stream_id: str, partition: str) -> str:
        return f"{self._prefix}:{stream_id}:{partition}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.stream_id, checkpoint.partition)
        data = json.dumps(checkpoint.to_dict())
        self._client.setex(key, self._ttl, data)

    def load(self, stream_id: str, partition: str) -> Optional[Checkpoint]:
        key = self._key(stream_id, partition)
        data = self._client.get(key)
        if data is None:
            return None
        if isinstance(data, bytes):
            data = data.decode()
        return Checkpoint.from_dict(json.loads(data))

    def delete(self, stream_id: str, partition: str) -> None:
        key = self._key(stream_id, partition)
        self._client.delete(key)

    def list_partitions(self, stream_id: str):
        pattern = self._key(stream_id, "*")
        keys = self._client.keys(pattern)
        prefix = self._key(stream_id, "")
        return [k.decode().removeprefix(prefix) if isinstance(k, bytes) else k.removeprefix(prefix) for k in keys]
