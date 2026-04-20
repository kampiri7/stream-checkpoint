import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class GarnetCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Microsoft Garnet (Redis-compatible).

    Args:
        client: A Garnet client instance (e.g. ``redis.Redis`` pointed at a
            Garnet server).  Any client that exposes ``set``, ``setex``,
            ``get``, ``delete``, and ``scan`` is accepted.
        prefix: Optional key prefix applied to every checkpoint key.
        ttl: Optional time-to-live in seconds.  When set every key is stored
            with an expiry.
    """

    def __init__(self, client, *, prefix: str = "checkpoint:", ttl: Optional[int] = None):
        self._client = client
        self._prefix = prefix
        self._ttl = ttl

    def _key(self, pipeline_id: str, checkpoint_id: str) -> str:
        return f"{self._prefix}{pipeline_id}:{checkpoint_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.checkpoint_id)
        value = json.dumps(checkpoint.to_dict())
        if self._ttl:
            self._client.setex(key, self._ttl, value)
        else:
            self._client.set(key, value)

    def load(self, pipeline_id: str, checkpoint_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, checkpoint_id)
        raw = self._client.get(key)
        if raw is None:
            return None
        data = json.loads(raw)
        return Checkpoint.from_dict(data)

    def delete(self, pipeline_id: str, checkpoint_id: str) -> None:
        key = self._key(pipeline_id, checkpoint_id)
        self._client.delete(key)

    def list_checkpoints(self, pipeline_id: str):
        pattern = f"{self._prefix}{pipeline_id}:*"
        cursor = 0
        results = []
        while True:
            cursor, keys = self._client.scan(cursor, match=pattern, count=100)
            for key in keys:
                raw = self._client.get(key)
                if raw is not None:
                    results.append(Checkpoint.from_dict(json.loads(raw)))
            if cursor == 0:
                break
        return results
