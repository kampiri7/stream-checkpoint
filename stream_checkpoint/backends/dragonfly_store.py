"""Dragonfly backend for stream-checkpoint.

Dragonfly is a modern, high-performance in-memory data store that is
API-compatible with Redis. This backend reuses the same protocol.
"""
from __future__ import annotations

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class DragonflyCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by a Dragonfly instance.

    Parameters
    ----------
    client:
        A Dragonfly (Redis-compatible) client, e.g. ``redis.Redis`` pointed
        at a Dragonfly server.
    prefix:
        Key prefix used to namespace all checkpoint keys.
    ttl:
        Optional time-to-live in seconds.  When set, keys expire
        automatically after this many seconds.
    """

    def __init__(self, client, prefix: str = "checkpoint", ttl: Optional[int] = None):
        self._client = client
        self._prefix = prefix
        self._ttl = ttl

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}:{pipeline_id}:{stream_id}"

    # ------------------------------------------------------------------
    # BaseCheckpointStore interface
    # ------------------------------------------------------------------

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        value = json.dumps(checkpoint.to_dict())
        if self._ttl:
            self._client.setex(key, self._ttl, value)
        else:
            self._client.set(key, value)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, stream_id)
        raw = self._client.get(key)
        if raw is None:
            return None
        data = json.loads(raw)
        return Checkpoint.from_dict(data)

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
                checkpoints.append(Checkpoint.from_dict(json.loads(raw)))
        return checkpoints
