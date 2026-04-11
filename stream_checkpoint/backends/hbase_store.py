import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint

_COLUMN_FAMILY = b"cf"
_DATA_COLUMN = b"cf:data"


class HBaseCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Apache HBase via happybase."""

    def __init__(self, connection, table_name: str = "stream_checkpoints"):
        self.connection = connection
        self.table_name = table_name
        self._ensure_table()

    def _ensure_table(self) -> None:
        existing = [
            t.decode() if isinstance(t, bytes) else t
            for t in self.connection.tables()
        ]
        if self.table_name not in existing:
            self.connection.create_table(
                self.table_name, {_COLUMN_FAMILY.decode(): {}}
            )

    def _row_key(self, pipeline_id: str, stream_id: str) -> bytes:
        return f"{pipeline_id}:{stream_id}".encode()

    def save(self, checkpoint: Checkpoint) -> None:
        table = self.connection.table(self.table_name)
        data = json.dumps(checkpoint.to_dict()).encode()
        table.put(self._row_key(checkpoint.pipeline_id, checkpoint.stream_id),
                  {_DATA_COLUMN: data})

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        table = self.connection.table(self.table_name)
        row = table.row(self._row_key(pipeline_id, stream_id))
        if not row:
            return None
        return Checkpoint.from_dict(json.loads(row[_DATA_COLUMN].decode()))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        table = self.connection.table(self.table_name)
        table.delete(self._row_key(pipeline_id, stream_id))

    def list_checkpoints(self, pipeline_id: str):
        table = self.connection.table(self.table_name)
        prefix = f"{pipeline_id}:".encode()
        results = []
        for _key, row in table.scan(row_prefix=prefix):
            results.append(Checkpoint.from_dict(json.loads(row[_DATA_COLUMN].decode())))
        return results
