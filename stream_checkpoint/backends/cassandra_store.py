import json
from datetime import datetime
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class CassandraCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Apache Cassandra."""

    def __init__(
        self,
        session,
        keyspace: str = "stream_checkpoint",
        table: str = "checkpoints",
        ttl: Optional[int] = None,
    ):
        self.session = session
        self.keyspace = keyspace
        self.table = table
        self.ttl = ttl
        self._create_table()

    def _create_table(self) -> None:
        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.keyspace}.{self.table} (
                pipeline_id TEXT,
                stream_id   TEXT,
                data        TEXT,
                PRIMARY KEY (pipeline_id, stream_id)
            )
            """
        )

    def save(self, checkpoint: Checkpoint) -> None:
        data = json.dumps(checkpoint.to_dict())
        if self.ttl:
            self.session.execute(
                f"INSERT INTO {self.keyspace}.{self.table} "
                f"(pipeline_id, stream_id, data) VALUES (%s, %s, %s) USING TTL %s",
                (checkpoint.pipeline_id, checkpoint.stream_id, data, self.ttl),
            )
        else:
            self.session.execute(
                f"INSERT INTO {self.keyspace}.{self.table} "
                f"(pipeline_id, stream_id, data) VALUES (%s, %s, %s)",
                (checkpoint.pipeline_id, checkpoint.stream_id, data),
            )

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        row = self.session.execute(
            f"SELECT data FROM {self.keyspace}.{self.table} "
            f"WHERE pipeline_id = %s AND stream_id = %s",
            (pipeline_id, stream_id),
        ).one()
        if row is None:
            return None
        return Checkpoint.from_dict(json.loads(row.data))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        self.session.execute(
            f"DELETE FROM {self.keyspace}.{self.table} "
            f"WHERE pipeline_id = %s AND stream_id = %s",
            (pipeline_id, stream_id),
        )

    def list_checkpoints(self, pipeline_id: str):
        rows = self.session.execute(
            f"SELECT data FROM {self.keyspace}.{self.table} "
            f"WHERE pipeline_id = %s",
            (pipeline_id,),
        )
        return [Checkpoint.from_dict(json.loads(row.data)) for row in rows]
