"""Hazelcast checkpoint store backend."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class HazelcastCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Hazelcast IMDG.

    Args:
        client: A Hazelcast client instance.
        map_name: Name of the Hazelcast IMap to use for storage.
        prefix: Optional key prefix applied to all checkpoint keys.
    """

    def __init__(self, client, map_name: str = "checkpoints", prefix: str = "ckpt"):
        self._client = client
        self._map = client.get_map(map_name).blocking()
        self._prefix = prefix

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}:{pipeline_id}:{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        """Persist a checkpoint to the Hazelcast map."""
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        self._map.set(key, json.dumps(checkpoint.to_dict()))

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        """Retrieve a checkpoint from the Hazelcast map."""
        key = self._key(pipeline_id, stream_id)
        value = self._map.get(key)
        if value is None:
            return None
        return Checkpoint.from_dict(json.loads(value))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        """Remove a checkpoint from the Hazelcast map."""
        key = self._key(pipeline_id, stream_id)
        self._map.delete(key)

    def list_checkpoints(self, pipeline_id: str) -> list:
        """List all checkpoints for a given pipeline."""
        prefix = f"{self._prefix}:{pipeline_id}:"
        results = []
        for key in self._map.key_set():
            if key.startswith(prefix):
                value = self._map.get(key)
                if value is not None:
                    results.append(Checkpoint.from_dict(json.loads(value)))
        return results

    def exists(self, pipeline_id: str, stream_id: str) -> bool:
        """Check whether a checkpoint exists without fetching its full value.

        Args:
            pipeline_id: The pipeline identifier.
            stream_id: The stream identifier.

        Returns:
            True if a checkpoint exists for the given pipeline/stream pair,
            False otherwise.
        """
        key = self._key(pipeline_id, stream_id)
        return self._map.contains_key(key)
