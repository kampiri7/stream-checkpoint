import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class CloudflareKVCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Cloudflare Workers KV via HTTP API."""

    def __init__(self, client, namespace_id: str, prefix: str = "checkpoint"):
        """
        :param client: Cloudflare KV client with put/get/delete methods.
        :param namespace_id: KV namespace identifier.
        :param prefix: Key prefix for all checkpoints.
        """
        self._client = client
        self._namespace_id = namespace_id
        self._prefix = prefix

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}:{pipeline_id}:{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        self._client.put(self._namespace_id, key, json.dumps(checkpoint.to_dict()))

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, stream_id)
        value = self._client.get(self._namespace_id, key)
        if value is None:
            return None
        return Checkpoint.from_dict(json.loads(value))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._key(pipeline_id, stream_id)
        self._client.delete(self._namespace_id, key)

    def list_checkpoints(self, pipeline_id: str) -> list:
        prefix = f"{self._prefix}:{pipeline_id}:"
        keys = self._client.list_keys(self._namespace_id, prefix=prefix)
        checkpoints = []
        for k in keys:
            value = self._client.get(self._namespace_id, k)
            if value is not None:
                checkpoints.append(Checkpoint.from_dict(json.loads(value)))
        return checkpoints
