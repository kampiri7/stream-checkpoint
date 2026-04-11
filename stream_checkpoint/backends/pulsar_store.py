"""Apache Pulsar checkpoint store backend."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class PulsarCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Apache Pulsar (via Pulsar Functions state store).

    Uses a Pulsar admin client or a key-value state table to persist checkpoints.
    Expects a client object with a ``get_table`` method returning a table that
    supports ``put``, ``get``, and ``delete`` keyed by string.
    """

    def __init__(self, client, table_name: str = "stream_checkpoints", prefix: str = "ckpt"):
        """
        Args:
            client: Pulsar client instance (or compatible fake).
            table_name: Name of the state table to use.
            prefix: Key prefix applied to all checkpoint keys.
        """
        self._client = client
        self._table_name = table_name
        self._prefix = prefix
        self._table = client.get_table(table_name)

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}:{pipeline_id}:{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        self._table.put(key, json.dumps(checkpoint.to_dict()))

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, stream_id)
        raw = self._table.get(key)
        if raw is None:
            return None
        return Checkpoint.from_dict(json.loads(raw))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._key(pipeline_id, stream_id)
        self._table.delete(key)

    def list_checkpoints(self, pipeline_id: str) -> list:
        prefix = f"{self._prefix}:{pipeline_id}:"
        return [
            Checkpoint.from_dict(json.loads(v))
            for k, v in self._table.items()
            if k.startswith(prefix)
        ]
