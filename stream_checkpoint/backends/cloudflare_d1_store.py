import json
from ..base import BaseCheckpointStore, Checkpoint


class CloudflareD1CheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Cloudflare D1 (SQLite-compatible edge database)."""

    def __init__(self, client, database_id: str, table: str = "checkpoints"):
        """
        :param client: A Cloudflare D1 HTTP client with an ``execute`` method.
        :param database_id: The D1 database identifier.
        :param table: Table name to store checkpoints in.
        """
        self._client = client
        self._database_id = database_id
        self._table = table
        self._create_table()

    def _create_table(self) -> None:
        self._client.execute(
            self._database_id,
            f"CREATE TABLE IF NOT EXISTS {self._table} "
            "(pipeline TEXT NOT NULL, stream TEXT NOT NULL, "
            "data TEXT NOT NULL, "
            "PRIMARY KEY (pipeline, stream))",
            [],
        )

    def save(self, checkpoint: Checkpoint) -> None:
        payload = json.dumps(checkpoint.to_dict())
        self._client.execute(
            self._database_id,
            f"INSERT INTO {self._table} (pipeline, stream, data) VALUES (?, ?, ?) "
            "ON CONFLICT(pipeline, stream) DO UPDATE SET data=excluded.data",
            [checkpoint.pipeline_id, checkpoint.stream_id, payload],
        )

    def load(self, pipeline_id: str, stream_id: str):
        result = self._client.execute(
            self._database_id,
            f"SELECT data FROM {self._table} WHERE pipeline=? AND stream=?",
            [pipeline_id, stream_id],
        )
        row = result.fetchone()
        if row is None:
            return None
        return Checkpoint.from_dict(json.loads(row[0]))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        self._client.execute(
            self._database_id,
            f"DELETE FROM {self._table} WHERE pipeline=? AND stream=?",
            [pipeline_id, stream_id],
        )

    def list_checkpoints(self, pipeline_id: str):
        result = self._client.execute(
            self._database_id,
            f"SELECT data FROM {self._table} WHERE pipeline=?",
            [pipeline_id],
        )
        return [Checkpoint.from_dict(json.loads(row[0])) for row in result.fetchall()]
