import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class SurrealDBCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by SurrealDB.

    Uses the SurrealDB Python client to persist checkpoints in a
    given namespace/database/table combination.

    Args:
        client: A connected ``surrealdb.SurrealDB`` (or compatible) client.
        namespace: SurrealDB namespace to use.
        database: SurrealDB database to use.
        table: Table (record type) name to store checkpoints in.
    """

    def __init__(
        self,
        client,
        namespace: str = "stream",
        database: str = "checkpoints",
        table: str = "checkpoint",
    ) -> None:
        self._client = client
        self._namespace = namespace
        self._database = database
        self._table = table
        # Switch to the target namespace/database
        self._client.use(self._namespace, self._database)

    def _record_id(self, stream_id: str, pipeline_id: str) -> str:
        """Build a deterministic SurrealDB record ID."""
        safe = f"{stream_id}__{pipeline_id}".replace(" ", "_")
        return f"{self._table}:{safe}"

    def save(self, checkpoint: Checkpoint) -> None:
        record_id = self._record_id(checkpoint.stream_id, checkpoint.pipeline_id)
        data = checkpoint.to_dict()
        # SurrealDB upsert via raw query
        self._client.query(
            f"UPDATE {record_id} CONTENT $data",
            {"data": data},
        )

    def load(self, stream_id: str, pipeline_id: str) -> Optional[Checkpoint]:
        record_id = self._record_id(stream_id, pipeline_id)
        result = self._client.query(f"SELECT * FROM {record_id}")
        # result is typically [{"result": [...], "status": "OK"}]
        if not result or not result[0].get("result"):
            return None
        row = result[0]["result"][0]
        return Checkpoint.from_dict(row)

    def delete(self, stream_id: str, pipeline_id: str) -> None:
        record_id = self._record_id(stream_id, pipeline_id)
        self._client.query(f"DELETE {record_id}")

    def list_checkpoints(self, pipeline_id: str) -> list:
        result = self._client.query(
            f"SELECT * FROM {self._table} WHERE pipeline_id = $pid",
            {"pid": pipeline_id},
        )
        if not result or not result[0].get("result"):
            return []
        return [Checkpoint.from_dict(row) for row in result[0]["result"]]
