import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class AerospikeCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Aerospike.

    Parameters
    ----------
    client:
        An ``aerospike.Client`` instance (already connected).
    namespace:
        Aerospike namespace to use (default: ``"checkpoints"``).
    set_name:
        Aerospike set name to use (default: ``"stream_checkpoint"``).
    ttl:
        Optional record TTL in seconds.  ``0`` means never expire.
    """

    def __init__(
        self,
        client,
        namespace: str = "checkpoints",
        set_name: str = "stream_checkpoint",
        ttl: int = 0,
    ) -> None:
        self._client = client
        self._namespace = namespace
        self._set = set_name
        self._ttl = ttl

    def _key(self, pipeline_id: str, stream_id: str) -> tuple:
        bin_key = f"{pipeline_id}:{stream_id}"
        return (self._namespace, self._set, bin_key)

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        data = {"payload": json.dumps(checkpoint.to_dict())}
        meta = {"ttl": self._ttl}
        policy = {"exists": self._client.POLICY_EXISTS_IGNORE}
        self._client.put(key, data, meta, policy)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, stream_id)
        try:
            _, _, record = self._client.get(key)
        except Exception:
            return None
        if record is None:
            return None
        return Checkpoint.from_dict(json.loads(record["payload"]))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._key(pipeline_id, stream_id)
        try:
            self._client.remove(key)
        except Exception:
            pass

    def list_checkpoints(self, pipeline_id: str):
        results = []
        scan = self._client.scan(self._namespace, self._set)

        def callback(record):
            _, _, bins = record
            if bins is None:
                return
            cp = Checkpoint.from_dict(json.loads(bins["payload"]))
            if cp.pipeline_id == pipeline_id:
                results.append(cp)

        scan.foreach(callback)
        return results
