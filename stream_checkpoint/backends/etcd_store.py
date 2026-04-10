"""etcd backend for stream-checkpoint."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class EtcdCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by etcd via the etcd3 client."""

    def __init__(self, client, prefix: str = "/checkpoints"):
        """
        Initialize the etcd checkpoint store.

        :param client: An ``etcd3`` client instance.
        :param prefix: Key prefix used for all checkpoint entries.
        """
        self._client = client
        self._prefix = prefix.rstrip("/")

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        """Build the etcd key for the given pipeline/stream pair."""
        return f"{self._prefix}/{pipeline_id}/{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        """Persist a checkpoint to etcd."""
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        value = json.dumps(checkpoint.to_dict())
        self._client.put(key, value)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        """Retrieve a checkpoint from etcd."""
        key = self._key(pipeline_id, stream_id)
        value, _ = self._client.get(key)
        if value is None:
            return None
        return Checkpoint.from_dict(json.loads(value))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        """Delete a checkpoint from etcd."""
        key = self._key(pipeline_id, stream_id)
        self._client.delete(key)

    def list_checkpoints(self, pipeline_id: str):
        """List all checkpoints for a given pipeline."""
        prefix = f"{self._prefix}/{pipeline_id}/"
        checkpoints = []
        for value, _ in self._client.get_prefix(prefix):
            if value is not None:
                checkpoints.append(Checkpoint.from_dict(json.loads(value)))
        return checkpoints
