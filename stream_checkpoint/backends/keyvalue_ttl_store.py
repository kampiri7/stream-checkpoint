"""Generic key-value store with TTL support backed by any dict-like client."""

import json
import time
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class KeyValueTTLCheckpointStore(BaseCheckpointStore):
    """Checkpoint store using a generic key-value client with TTL support.

    The client must expose:
        set(key, value, ttl_seconds)  -> None
        get(key)                      -> Optional[str]
        delete(key)                   -> None
        keys(prefix)                  -> Iterable[str]
    """

    def __init__(self, client, prefix: str = "checkpoint:", ttl: Optional[int] = None):
        self._client = client
        self._prefix = prefix
        self._ttl = ttl

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}{pipeline_id}:{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        payload = json.dumps(checkpoint.to_dict())
        if self._ttl:
            self._client.set(key, payload, ttl_seconds=self._ttl)
        else:
            self._client.set(key, payload, ttl_seconds=None)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, stream_id)
        raw = self._client.get(key)
        if raw is None:
            return None
        return Checkpoint.from_dict(json.loads(raw))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._key(pipeline_id, stream_id)
        self._client.delete(key)

    def list_checkpoints(self, pipeline_id: str) -> list:
        prefix = f"{self._prefix}{pipeline_id}:"
        keys = self._client.keys(prefix)
        results = []
        for k in keys:
            raw = self._client.get(k)
            if raw is not None:
                results.append(Checkpoint.from_dict(json.loads(raw)))
        return results
