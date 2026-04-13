"""Turso (libSQL) checkpoint store backend."""

import json
from ..base import BaseCheckpointStore, Checkpoint


class TursoCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Turso (libSQL) database.

    Args:
        client: A libsql_client.Client instance.
        table: Table name to use for checkpoints (default: 'checkpoints').
    """

    def __init__(self, client, table: str = "checkpoints"):
        self._client = client
        self._table = table
        self._create_table()

    def _create_table(self):
        self._client.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table} (
                stream_id TEXT NOT NULL,
                partition TEXT NOT NULL,
                offset TEXT NOT NULL,
                metadata TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (stream_id, partition)
            )
            """
        )

    def save(self, checkpoint: Checkpoint) -> None:
        data = checkpoint.to_dict()
        self._client.execute(
            f"""
            INSERT INTO {self._table} (stream_id, partition, offset, metadata, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(stream_id, partition) DO UPDATE SET
                offset = excluded.offset,
                metadata = excluded.metadata,
                updated_at = excluded.updated_at
            """,
            [
                data["stream_id"],
                data["partition"],
                data["offset"],
                json.dumps(data.get("metadata") or {}),
                data["updated_at"],
            ],
        )

    def load(self, stream_id: str, partition: str) -> Checkpoint | None:
        result = self._client.execute(
            f"SELECT stream_id, partition, offset, metadata, updated_at FROM {self._table} WHERE stream_id = ? AND partition = ?",
            [stream_id, partition],
        )
        rows = result.rows
        if not rows:
            return None
        row = rows[0]
        return Checkpoint.from_dict({
            "stream_id": row[0],
            "partition": row[1],
            "offset": row[2],
            "metadata": json.loads(row[3]) if row[3] else {},
            "updated_at": row[4],
        })

    def delete(self, stream_id: str, partition: str) -> None:
        self._client.execute(
            f"DELETE FROM {self._table} WHERE stream_id = ? AND partition = ?",
            [stream_id, partition],
        )

    def list_checkpoints(self, stream_id: str) -> list[Checkpoint]:
        result = self._client.execute(
            f"SELECT stream_id, partition, offset, metadata, updated_at FROM {self._table} WHERE stream_id = ?",
            [stream_id],
        )
        checkpoints = []
        for row in result.rows:
            checkpoints.append(Checkpoint.from_dict({
                "stream_id": row[0],
                "partition": row[1],
                "offset": row[2],
                "metadata": json.loads(row[3]) if row[3] else {},
                "updated_at": row[4],
            }))
        return checkpoints
