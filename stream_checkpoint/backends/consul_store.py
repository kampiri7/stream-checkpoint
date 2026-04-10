import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class ConsulCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by HashiCorp Consul KV.

    Requires the ``python-consul2`` package::

        pip install python-consul2
    """

    def __init__(self, client, prefix: str = "stream_checkpoint"):
        """
        :param client: A ``consul.Consul`` client instance.
        :param prefix: Key prefix used for all checkpoint entries.
        """
        self._client = client
        self._prefix = prefix.rstrip("/")

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}/{pipeline_id}/{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        value = json.dumps(checkpoint.to_dict())
        self._client.kv.put(key, value)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, stream_id)
        _, data = self._client.kv.get(key)
        if data is None:
            return None
        raw = data["Value"]
        if isinstance(raw, bytes):
            raw = raw.decode()
        return Checkpoint.from_dict(json.loads(raw))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._key(pipeline_id, stream_id)
        self._client.kv.delete(key)

    def list_checkpoints(self, pipeline_id: str):
        prefix = f"{self._prefix}/{pipeline_id}/"
        _, items = self._client.kv.get(prefix, recurse=True)
        if not items:
            return []
        return [
            Checkpoint.from_dict(json.loads(
                item["Value"].decode() if isinstance(item["Value"], bytes) else item["Value"]
            ))
            for item in items
        ]
