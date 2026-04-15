import json
from datetime import datetime
from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class KeyspacesCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by Amazon Keyspaces (Managed Cassandra).

    Uses the cassandra-driver under the hood, connecting via the
    Amazon Keyspaces endpoint with SigV4 authentication.

    Args:
        session: A connected cassandra.cluster.Session instance.
        keyspace: Keyspace (schema) name to use.
        table: Table name to use (default: "checkpoints").
    """

    def __init__(self, session, keyspace: str, table: str = "checkpoints"):
        self._session = session
        self._keyspace = keyspace
        self._table = table
        self._create_table()

    def _create_table(self) -> None:
        self._session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._keyspace}.{self._table} (
                stream_id text,
                partition_id text,
                offset text,
                metadata text,
                updated_at timestamp,
                PRIMARY KEY (stream_id, partition_id)
            )
            """
        )

    def save(self, checkpoint: Checkpoint) -> None:
        self._session.execute(
            f"""
            INSERT INTO {self._keyspace}.{self._table}
            (stream_id, partition_id, offset, metadata, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                checkpoint.stream_id,
                checkpoint.partition_id,
                checkpoint.offset,
                json.dumps(checkpoint.metadata or {}),
                checkpoint.updated_at or datetime.utcnow(),
            ),
        )

    def load(self, stream_id: str, partition_id: str) -> Checkpoint | None:
        row = self._session.execute(
            f"SELECT * FROM {self._keyspace}.{self._table} "
            f"WHERE stream_id=%s AND partition_id=%s",
            (stream_id, partition_id),
        ).one()
        if row is None:
            return None
        return Checkpoint(
            stream_id=row.stream_id,
            partition_id=row.partition_id,
            offset=row.offset,
            metadata=json.loads(row.metadata),
            updated_at=row.updated_at,
        )

    def delete(self, stream_id: str, partition_id: str) -> None:
        self._session.execute(
            f"DELETE FROM {self._keyspace}.{self._table} "
            f"WHERE stream_id=%s AND partition_id=%s",
            (stream_id, partition_id),
        )

    def list_checkpoints(self, stream_id: str) -> list[Checkpoint]:
        rows = self._session.execute(
            f"SELECT * FROM {self._keyspace}.{self._table} WHERE stream_id=%s",
            (stream_id,),
        )
        return [
            Checkpoint(
                stream_id=r.stream_id,
                partition_id=r.partition_id,
                offset=r.offset,
                metadata=json.loads(r.metadata),
                updated_at=r.updated_at,
            )
            for r in rows
        ]
