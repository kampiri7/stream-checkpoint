"""Generic key-value store backend using a simple dict-like interface."""
import json
from typing import Optional
from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class KVCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by any dict-like key-value client.

    The client must support:
      - client[key] = value  (set)
      - client[key]          (get, raises KeyError if missing)
      - del client[key]      (delete)
      - key in client        (existence check)
      - iter(client)         (iteration over keys)
    """

    def __init__(self, client, prefix: str = "checkpoint:"):
        self._client = client
        self._prefix = prefix

    def _key(self, pipeline_id: str, partition: str) -> str:
        return f"{self._prefix}{pipeline_id}:{partition}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.partition)
        self._client[key] = json.dumps(checkpoint.to_dict())

    def load(self, pipeline_id: str, partition: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, partition)
        try:
            raw = self._client[key]
        except KeyError:
            return None
        return Checkpoint.from_dict(json.loads(raw))

    def delete(self, pipeline_id: str, partition: str) -> None:
        key = self._key(pipeline_id, partition)
        try:
            del self._client[key]
        except KeyError:
            pass

    def list_checkpoints(self, pipeline_id: str):
        prefix = f"{self._prefix}{pipeline_id}:"
        results = []
        for key in self._client:
            if key.startswith(prefix):
                raw = self._client[key]
                results.append(Checkpoint.from_dict(json.loads(raw)))
        return results
