import json
from typing import Optional, List

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class ScyllaDBCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by ScyllaDB (Cassandra-compatible).

    Uses the cassandra-driver. Checkpoints are stored in a dedicated
    keyspace and table.
    """

    def __init__(
        self,
        session,
        keyspace: str = "stream_checkpoints",
        table: str = "checkpoints",
    ):
        self._session = session
        self._keyspace = keyspace
        self._table = table
        self._create_table()

    def _create_table(self) -> None:
        self._session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {self._keyspace} "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        self._session.execute(
            f"CREATE TABLE IF NOT EXISTS {self._keyspace}.{self._table} ("
            "pipeline_id TEXT, "
            "stream_id TEXT, "
            "data TEXT, "
            "PRIMARY KEY (pipeline_id, stream_id)"
            ")"
        )

    def save(self, checkpoint: Checkpoint) -> None:
        payload = json.dumps(checkpoint.to_dict())
        self._session.execute(
            f"INSERT INTO {self._keyspace}.{self._table} "
            "(pipeline_id, stream_id, data) VALUES (%s, %s, %s)",
            (checkpoint.pipeline_id, checkpoint.stream_id, payload),
        )

    def load(
        self, pipeline_id: str, stream_id: str
    ) -> Optional[Checkpoint]:
        row = self._session.execute(
            f"SELECT data FROM {self._keyspace}.{self._table} "
            "WHERE pipeline_id = %s AND stream_id = %s",
            (pipeline_id, stream_id),
        ).one()
        if row is None:
            return None
        return Checkpoint.from_dict(json.loads(row.data))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        self._session.execute(
            f"DELETE FROM {self._keyspace}.{self._table} "
            "WHERE pipeline_id = %s AND stream_id = %s",
            (pipeline_id, stream_id),
        )

    def list_checkpoints(self, pipeline_id: str) -> List[Checkpoint]:
        rows = self._session.execute(
            f"SELECT data FROM {self._keyspace}.{self._table} "
            "WHERE pipeline_id = %s",
            (pipeline_id,),
        )
        return [Checkpoint.from_dict(json.loads(r.data)) for r in rows]
