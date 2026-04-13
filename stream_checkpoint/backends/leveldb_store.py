import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class LevelDBCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by LevelDB via the leveldb or plyvel package."""

    def __init__(self, db_path: str, prefix: str = "checkpoint:"):
        """
        Args:
            db_path: Path to the LevelDB database directory.
            prefix: Key prefix applied to all checkpoint keys.
        """
        import plyvel  # type: ignore

        self._db = plyvel.DB(db_path, create_if_missing=True)
        self._prefix = prefix

    def _key(self, pipeline_id: str, stream_id: str) -> bytes:
        return f"{self._prefix}{pipeline_id}:{stream_id}".encode()

    def save(self, checkpoint: Checkpoint) -> None:
        raw = json.dumps(checkpoint.to_dict()).encode()
        self._db.put(self._key(checkpoint.pipeline_id, checkpoint.stream_id), raw)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        raw = self._db.get(self._key(pipeline_id, stream_id))
        if raw is None:
            return None
        return Checkpoint.from_dict(json.loads(raw.decode()))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        self._db.delete(self._key(pipeline_id, stream_id))

    def list_checkpoints(self, pipeline_id: str) -> list:
        prefix = f"{self._prefix}{pipeline_id}:".encode()
        results = []
        with self._db.iterator(prefix=prefix) as it:
            for _, value in it:
                results.append(Checkpoint.from_dict(json.loads(value.decode())))
        return results

    def close(self) -> None:
        self._db.close()
