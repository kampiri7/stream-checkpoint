import json
from datetime import datetime, timezone
from stream_checkpoint.base import Checkpoint, BaseCheckpointStore


class VoltDBCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by VoltDB.

    Uses a JDBC-style Python client (voltdbclient / voltdb) to persist
    checkpoints in a single table.
    """

    def __init__(self, client, table: str = "checkpoints"):
        """
        :param client: A VoltDB client instance (voltdb.FastSerializer or
                       any object exposing .callProcedure()).
        :param table:  Table name used to store checkpoints.
        """
        self._client = client
        self._table = table
        self._create_table()

    def _create_table(self) -> None:
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {self._table} ("
            "pipeline_id VARCHAR(255) NOT NULL, "
            "stream_id   VARCHAR(255) NOT NULL, "
            "offset      VARCHAR(4096) NOT NULL, "
            "metadata    VARCHAR(4096), "
            "updated_at  TIMESTAMP NOT NULL, "
            "PRIMARY KEY (pipeline_id, stream_id)"
            ");"
        )
        self._client.execute(ddl)

    def save(self, checkpoint: Checkpoint) -> None:
        upsert = (
            f"UPSERT INTO {self._table} "
            "(pipeline_id, stream_id, offset, metadata, updated_at) "
            "VALUES (?, ?, ?, ?, ?);"
        )
        self._client.execute(
            upsert,
            [
                checkpoint.pipeline_id,
                checkpoint.stream_id,
                json.dumps(checkpoint.offset),
                json.dumps(checkpoint.metadata) if checkpoint.metadata else None,
                checkpoint.updated_at.isoformat(),
            ],
        )

    def load(self, pipeline_id: str, stream_id: str):
        query = (
            f"SELECT pipeline_id, stream_id, offset, metadata, updated_at "
            f"FROM {self._table} "
            "WHERE pipeline_id = ? AND stream_id = ?;"
        )
        row = self._client.fetchone(query, [pipeline_id, stream_id])
        if row is None:
            return None
        return Checkpoint(
            pipeline_id=row[0],
            stream_id=row[1],
            offset=json.loads(row[2]),
            metadata=json.loads(row[3]) if row[3] else None,
            updated_at=datetime.fromisoformat(row[4]),
        )

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        self._client.execute(
            f"DELETE FROM {self._table} WHERE pipeline_id = ? AND stream_id = ?;",
            [pipeline_id, stream_id],
        )

    def list_checkpoints(self, pipeline_id: str):
        query = (
            f"SELECT pipeline_id, stream_id, offset, metadata, updated_at "
            f"FROM {self._table} WHERE pipeline_id = ?;"
        )
        rows = self._client.fetchall(query, [pipeline_id])
        return [
            Checkpoint(
                pipeline_id=r[0],
                stream_id=r[1],
                offset=json.loads(r[2]),
                metadata=json.loads(r[3]) if r[3] else None,
                updated_at=datetime.fromisoformat(r[4]),
            )
            for r in rows
        ]
