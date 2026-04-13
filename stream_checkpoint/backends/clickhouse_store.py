import json
from datetime import datetime, timezone
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class ClickHouseCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by ClickHouse.

    Requires the ``clickhouse-driver`` package::

        pip install clickhouse-driver
    """

    def __init__(
        self,
        client,
        database: str = "default",
        table: str = "checkpoints",
    ) -> None:
        self._client = client
        self._database = database
        self._table = table
        self._create_table()

    def _create_table(self) -> None:
        self._client.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._database}.{self._table} (
                stream_id  String,
                offset     String,
                metadata   String,
                updated_at DateTime
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY stream_id
            """
        )

    def save(self, checkpoint: Checkpoint) -> None:
        self._client.execute(
            f"""
            INSERT INTO {self._database}.{self._table}
                (stream_id, offset, metadata, updated_at)
            VALUES
            """,
            [
                {
                    "stream_id": checkpoint.stream_id,
                    "offset": checkpoint.offset,
                    "metadata": json.dumps(checkpoint.metadata or {}),
                    "updated_at": checkpoint.updated_at
                    or datetime.now(timezone.utc),
                }
            ],
        )

    def load(self, stream_id: str) -> Optional[Checkpoint]:
        rows = self._client.execute(
            f"""
            SELECT stream_id, offset, metadata, updated_at
            FROM {self._database}.{self._table} FINAL
            WHERE stream_id = %(stream_id)s
            LIMIT 1
            """,
            {"stream_id": stream_id},
        )
        if not rows:
            return None
        stream_id_, offset, metadata_raw, updated_at = rows[0]
        return Checkpoint(
            stream_id=stream_id_,
            offset=offset,
            metadata=json.loads(metadata_raw),
            updated_at=updated_at,
        )

    def delete(self, stream_id: str) -> None:
        self._client.execute(
            f"ALTER TABLE {self._database}.{self._table}"
            f" DELETE WHERE stream_id = %(stream_id)s",
            {"stream_id": stream_id},
        )

    def list_streams(self) -> list:
        rows = self._client.execute(
            f"SELECT DISTINCT stream_id FROM {self._database}.{self._table} FINAL"
        )
        return [r[0] for r in rows]
