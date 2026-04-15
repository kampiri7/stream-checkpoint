"""Redict checkpoint store backend.

Redict is a LGPL-licensed fork of Redis. This backend uses the same
protocol as Redis and is compatible with redis-py client.
"""

from __future__ import annotations

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class RedictCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Redict (Redis-compatible).

    Parameters
    ----------
    client:
        A Redict (redis-py compatible) client instance.
    prefix:
        Key prefix applied to all checkpoint keys. Defaults to
        ``"checkpoint:"``.
    ttl:
        Optional time-to-live in seconds for each key. When *None*
        keys never expire.
    """

    def __init__(self, client, prefix: str = "checkpoint:", ttl: Optional[int] = None) -> None:
        self._client = client
        self._prefix = prefix
        self._ttl = ttl

    def _key(self, stream_id: str, partition: str) -> str:
        return f"{self._prefix}{stream_id}:{partition}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.stream_id, checkpoint.partition)
        value = json.dumps(checkpoint.to_dict())
        if self._ttl is not None:
            self._client.setex(key, self._ttl, value)
        else:
            self._client.set(key, value)

    def load(self, stream_id: str, partition: str) -> Optional[Checkpoint]:
        key = self._key(stream_id, partition)
        raw = self._client.get(key)
        if raw is None:
            return None
        data = json.loads(raw)
        return Checkpoint.from_dict(data)

    def delete(self, stream_id: str, partition: str) -> None:
        key = self._key(stream_id, partition)
        self._client.delete(key)

    def list_checkpoints(self, stream_id: str) -> list[Checkpoint]:
        pattern = self._key(stream_id, "*")
        keys = self._client.keys(pattern)
        checkpoints = []
        for key in keys:
            raw = self._client.get(key)
            if raw is not None:
                checkpoints.append(Checkpoint.from_dict(json.loads(raw)))
        return checkpoints
