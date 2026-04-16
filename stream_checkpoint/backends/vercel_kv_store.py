import json
from stream_checkpoint.base import Checkpoint, BaseCheckpointStore


class VercelKVCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Vercel KV (Upstash Redis-compatible REST API)."""

    def __init__(self, client, prefix: str = "checkpoint"):
        """
        :param client: A Vercel KV REST client with set/get/delete methods.
        :param prefix: Key prefix for namespacing checkpoints.
        """
        self._client = client
        self._prefix = prefix

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}:{pipeline_id}:{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        self._client.set(key, json.dumps(checkpoint.to_dict()))

    def load(self, pipeline_id: str, stream_id: str):
        key = self._key(pipeline_id, stream_id)
        data = self._client.get(key)
        if data is None:
            return None
        return Checkpoint.from_dict(json.loads(data))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._key(pipeline_id, stream_id)
        self._client.delete(key)

    def list_checkpoints(self, pipeline_id: str):
        pattern = f"{self._prefix}:{pipeline_id}:*"
        keys = self._client.keys(pattern)
        checkpoints = []
        for key in keys:
            data = self._client.get(key)
            if data is not None:
                checkpoints.append(Checkpoint.from_dict(json.loads(data)))
        return checkpoints
