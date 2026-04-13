"""LMDB (Lightning Memory-Mapped Database) backend for stream-checkpoint."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class LMDBCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by LMDB.

    Args:
        path: Directory path for the LMDB environment.
        map_size: Maximum size of the database in bytes (default: 10 MB).
        namespace: Optional key namespace/prefix.
        client: Optional pre-created lmdb.Environment (for testing).
    """

    def __init__(
        self,
        path: str = "./checkpoints.lmdb",
        map_size: int = 10 * 1024 * 1024,
        namespace: str = "checkpoint",
        client=None,
    ):
        self.path = path
        self.map_size = map_size
        self.namespace = namespace
        if client is not None:
            self._env = client
        else:
            import lmdb
            self._env = lmdb.open(path, map_size=map_size)

    def _key(self, stream_id: str, pipeline_id: str) -> bytes:
        return f"{self.namespace}:{pipeline_id}:{stream_id}".encode()

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.stream_id, checkpoint.pipeline_id)
        value = json.dumps(checkpoint.to_dict()).encode()
        with self._env.begin(write=True) as txn:
            txn.put(key, value)

    def load(self, stream_id: str, pipeline_id: str) -> Optional[Checkpoint]:
        key = self._key(stream_id, pipeline_id)
        with self._env.begin() as txn:
            raw = txn.get(key)
        if raw is None:
            return None
        return Checkpoint.from_dict(json.loads(raw.decode()))

    def delete(self, stream_id: str, pipeline_id: str) -> None:
        key = self._key(stream_id, pipeline_id)
        with self._env.begin(write=True) as txn:
            txn.delete(key)

    def list_checkpoints(self, pipeline_id: str) -> list:
        prefix = f"{self.namespace}:{pipeline_id}:".encode()
        results = []
        with self._env.begin() as txn:
            cursor = txn.cursor()
            if cursor.set_range(prefix):
                while cursor.key().startswith(prefix):
                    results.append(Checkpoint.from_dict(json.loads(cursor.value().decode())))
                    if not cursor.next():
                        break
        return results

    def close(self) -> None:
        self._env.close()
