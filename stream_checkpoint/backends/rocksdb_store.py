"""RocksDB checkpoint store backend."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class RocksDBCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by RocksDB via python-rocksdb."""

    def __init__(self, db_path: str, prefix: str = "checkpoint:") -> None:
        """
        Args:
            db_path: Path on disk where the RocksDB database will be stored.
            prefix:  Key prefix applied to every checkpoint key.
        """
        import rocksdb  # type: ignore

        opts = rocksdb.Options()
        opts.create_if_missing = True
        self._db = rocksdb.DB(db_path, opts)
        self._prefix = prefix

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _key(self, pipeline_id: str, stream_id: str) -> bytes:
        return f"{self._prefix}{pipeline_id}:{stream_id}".encode()

    # ------------------------------------------------------------------
    # BaseCheckpointStore interface
    # ------------------------------------------------------------------

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        self._db.put(key, json.dumps(checkpoint.to_dict()).encode())

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        raw = self._db.get(self._key(pipeline_id, stream_id))
        if raw is None:
            return None
        return Checkpoint.from_dict(json.loads(raw.decode()))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        self._db.delete(self._key(pipeline_id, stream_id))

    def list_checkpoints(self, pipeline_id: str) -> list:
        prefix = f"{self._prefix}{pipeline_id}:".encode()
        it = self._db.itervalues()
        it.seek(prefix)
        results = []
        for raw in it:
            data = json.loads(raw.decode())
            if data.get("pipeline_id") == pipeline_id:
                results.append(Checkpoint.from_dict(data))
            else:
                break
        return results

    def close(self) -> None:
        """Release the RocksDB handle."""
        del self._db
