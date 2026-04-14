import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class TarantoolCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Tarantool in-memory database.

    Args:
        client: A ``tarantool.Connection`` instance (or compatible object).
        space: Name of the Tarantool space used to store checkpoints.
            The space must have a primary key on the first field (``key``).
        prefix: Optional key prefix to namespace checkpoints.
    """

    def __init__(self, client, space: str = "checkpoints", prefix: str = "ckpt") -> None:
        self._client = client
        self._space = space
        self._prefix = prefix

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}:{pipeline_id}:{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        value = json.dumps(checkpoint.to_dict())
        # REPLACE inserts or updates the tuple with the given primary key.
        self._client.space(self._space).replace((key, value))

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, stream_id)
        result = self._client.space(self._space).select(key)
        if not result or len(result) == 0:
            return None
        # result[0] is the first tuple; index 1 is the JSON value field.
        row = result[0]
        return Checkpoint.from_dict(json.loads(row[1]))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._key(pipeline_id, stream_id)
        self._client.space(self._space).delete(key)

    def list_checkpoints(self, pipeline_id: str) -> list:
        prefix = f"{self._prefix}:{pipeline_id}:"
        result = self._client.space(self._space).select()
        checkpoints = []
        for row in result:
            if row[0].startswith(prefix):
                checkpoints.append(Checkpoint.from_dict(json.loads(row[1])))
        return checkpoints
