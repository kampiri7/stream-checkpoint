"""Upstash Redis checkpoint store backend."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class UpstashCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Upstash Redis (REST API client).

    Parameters
    ----------
    client:
        An Upstash Redis client instance (e.g. ``upstash_redis.Redis``).
    prefix:
        Key prefix applied to every checkpoint key. Defaults to
        ``"checkpoint"``.
    ttl:
        Optional time-to-live in seconds.  When provided every key is
        stored with ``EXPIRE``.  Pass ``None`` (default) for no
        expiration.
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
        return Checkpoint.from_dict(json.loads(raw))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        self._client.delete(self._key(pipeline_id, stream_id))

    def list_checkpoints(self, pipeline_id: str):
        pattern = self._key(pipeline_id, "*")
        keys = self._client.keys(pattern)
        checkpoints = []
        for key in keys:
            raw = self._client.get(key)
            if raw is not None:
                checkpoints.append(Checkpoint.from_dict(json.loads(raw)))
        return checkpoints
