import json
from typing import Optional
from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class NATSCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by NATS JetStream key-value bucket.

    Args:
        kv_bucket: A NATS JetStream KeyValue bucket object (e.g. from
            ``nats.js.key_value(bucket_name)``).
        prefix: Optional key prefix applied to every checkpoint key.
    """

    def __init__(self, kv_bucket, prefix: str = "checkpoint") -> None:
        self._kv = kv_bucket
        self._prefix = prefix

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}.{pipeline_id}.{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        payload = json.dumps(checkpoint.to_dict()).encode()
        self._kv.put(key, payload)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, stream_id)
        try:
            entry = self._kv.get(key)
        except Exception:
            return None
        if entry is None:
            return None
        data = json.loads(entry.value.decode())
        return Checkpoint.from_dict(data)

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._key(pipeline_id, stream_id)
        try:
            self._kv.delete(key)
        except Exception:
            pass

    def list_checkpoints(self, pipeline_id: str):
        prefix = f"{self._prefix}.{pipeline_id}."
        try:
            keys = self._kv.keys()
        except Exception:
            return []
        results = []
        for key in keys:
            if key.startswith(prefix):
                entry = self._kv.get(key)
                if entry is not None:
                    data = json.loads(entry.value.decode())
                    results.append(Checkpoint.from_dict(data))
        return results
