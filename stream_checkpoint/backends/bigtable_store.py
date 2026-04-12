"""Google Cloud Bigtable checkpoint store backend."""

import json
from datetime import datetime, timezone

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class BigtableCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Google Cloud Bigtable.

    Args:
        instance: A google.cloud.bigtable.Instance object.
        table_id: The Bigtable table ID to use for storing checkpoints.
        column_family: Column family name (default: ``"cf1"``).
        column: Column qualifier name (default: ``"data"``).
        prefix: Optional key prefix for namespacing checkpoints.
    """

    def __init__(
        self,
        instance,
        table_id: str,
        column_family: str = "cf1",
        column: str = "data",
        prefix: str = "",
    ):
        self._table = instance.table(table_id)
        self._cf = column_family
        self._col = column.encode()
        self._prefix = prefix

    def _row_key(self, stream_id: str, partition: str) -> bytes:
        raw = f"{self._prefix}{stream_id}:{partition}"
        return raw.encode()

    def save(self, checkpoint: Checkpoint) -> None:
        row_key = self._row_key(checkpoint.stream_id, checkpoint.partition)
        row = self._table.direct_row(row_key)
        payload = json.dumps(checkpoint.to_dict()).encode()
        row.set_cell(self._cf, self._col, payload)
        row.commit()

    def load(self, stream_id: str, partition: str) -> Checkpoint | None:
        row_key = self._row_key(stream_id, partition)
        row = self._table.read_row(row_key)
        if row is None:
            return None
        cell = row.cells[self._cf][self._col][0]
        data = json.loads(cell.value.decode())
        return Checkpoint.from_dict(data)

    def delete(self, stream_id: str, partition: str) -> None:
        row_key = self._row_key(stream_id, partition)
        row = self._table.direct_row(row_key)
        row.delete()
        row.commit()

    def list_checkpoints(self, stream_id: str) -> list[Checkpoint]:
        prefix = f"{self._prefix}{stream_id}:".encode()
        rows = self._table.read_rows(
            start_key=prefix,
            end_key=prefix[:-1] + bytes([prefix[-1] + 1]),
        )
        checkpoints = []
        for row in rows:
            cell = row.cells[self._cf][self._col][0]
            data = json.loads(cell.value.decode())
            checkpoints.append(Checkpoint.from_dict(data))
        return checkpoints
