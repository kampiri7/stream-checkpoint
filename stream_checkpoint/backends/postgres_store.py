import json
from typing import Optional
from datetime import datetime, timezone

from stream_checkpoint.base import Checkpoint, BaseCheckpointStore


class PostgresCheckpointStore(BaseCheckpointStore):
    """
    PostgreSQL-backed checkpoint store using psycopg2.

    Table schema (auto-created if not exists):
        checkpoints(pipeline_id TEXT, checkpoint_id TEXT, data JSONB, updated_at TIMESTAMPTZ)
    """

    def __init__(self, connection, table: str = "checkpoints"):
        """
        :param connection: A psycopg2 connection object.
        :param table: Name of the table to store checkpoints in.
        """
        self._conn = connection
        self._table = table
        self._create_table()

    def _create_table(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._table} (
                    pipeline_id  TEXT        NOT NULL,
                    checkpoint_id TEXT       NOT NULL,
                    data         JSONB       NOT NULL,
                    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (pipeline_id, checkpoint_id)
                )
                """
            )
        self._conn.commit()

    def save(self, checkpoint: Checkpoint) -> None:
        payload = json.dumps(checkpoint.to_dict())
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._table} (pipeline_id, checkpoint_id, data, updated_at)
                VALUES (%s, %s, %s::jsonb, %s)
                ON CONFLICT (pipeline_id, checkpoint_id)
                DO UPDATE SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at
                """,
                (
                    checkpoint.pipeline_id,
                    checkpoint.checkpoint_id,
                    payload,
                    datetime.now(tz=timezone.utc),
                ),
            )
        self._conn.commit()

    def load(self, pipeline_id: str, checkpoint_id: str) -> Optional[Checkpoint]:
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT data FROM {self._table} WHERE pipeline_id = %s AND checkpoint_id = %s",
                (pipeline_id, checkpoint_id),
            )
            row = cur.fetchone()
        if row is None:
            return None
        data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
        return Checkpoint.from_dict(data)

    def delete(self, pipeline_id: str, checkpoint_id: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self._table} WHERE pipeline_id = %s AND checkpoint_id = %s",
                (pipeline_id, checkpoint_id),
            )
        self._conn.commit()

    def list_checkpoints(self, pipeline_id: str):
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT data FROM {self._table} WHERE pipeline_id = %s ORDER BY updated_at ASC",
                (pipeline_id,),
            )
            rows = cur.fetchall()
        return [
            Checkpoint.from_dict(r[0] if isinstance(r[0], dict) else json.loads(r[0]))
            for r in rows
        ]
