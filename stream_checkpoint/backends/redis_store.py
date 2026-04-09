"""Redis-backed checkpoint store implementation."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint

try:
    import redis
except ImportError as e:
    raise ImportError(
        "redis package is required for RedisCheckpointStore. "
        "Install it with: pip install redis"
    ) from e


class RedisCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Redis.

    Checkpoints are stored as JSON strings under keys of the form
    ``{prefix}:{pipeline_id}:{stream_id}``.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        prefix: str = "stream_checkpoint",
        ttl: Optional[int] = None,
        client: Optional["redis.Redis"] = None,
    ) -> None:
        self._prefix = prefix
        self._ttl = ttl
        self._client = client or redis.Redis(
            host=host, port=port, db=db, decode_responses=True
        )

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
        return Checkpoint.from_dict(json.loads(raw))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._key(pipeline_id, stream_id)
        self._client.delete(key)

    def exists(self, pipeline_id: str, stream_id: str) -> bool:
        key = self._key(pipeline_id, stream_id)
        return bool(self._client.exists(key))
