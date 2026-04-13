"""Neon (serverless Postgres) checkpoint store backend."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class NeonCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Neon serverless Postgres.

    Args:
        connection_string: Neon connection string (postgresql://...).
        table: Table name to store checkpoints in.
    """

    def __init__(self, connection_string: str, table: str = "checkpoints") -> None:
        import psycopg2  # type: ignore

        self._table = table
        self._conn = psycopg2.connect(connection_string)
        self._conn.autocommit = True
        self._create_table()

    def _create_table(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._table} (
                    stream_id TEXT NOT NULL,
                    partition_id TEXT NOT NULL,
                    offset TEXT NOT NULL,
                    metadata JSONB,
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (stream_id, partition_id)
                )
                """
            )

    def save(self, checkpoint: Checkpoint) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._table} (stream_id, partition_id, offset, metadata, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (stream_id, partition_id)
                DO UPDATE SET offset = EXCLUDED.offset,
                              metadata = EXCLUDED.metadata,
                              updated_at = NOW()
                """,
                (
                    checkpoint.stream_id,
                    checkpoint.partition_id,
                    checkpoint.offset,
                    json.dumps(checkpoint.metadata) if checkpoint.metadata else None,
                ),
            )

    def load(self, stream_id: str, partition_id: str) -> Optional[Checkpoint]:
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT stream_id, partition_id, offset, metadata FROM {self._table}"
                " WHERE stream_id = %s AND partition_id = %s",
                (stream_id, partition_id),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return Checkpoint(
            stream_id=row[0],
            partition_id=row[1],
            offset=row[2],
            metadata=row[3] if row[3] else {},
        )

    def delete(self, stream_id: str, partition_id: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self._table} WHERE stream_id = %s AND partition_id = %s",
                (stream_id, partition_id),
            )

    def list_checkpoints(self, stream_id: str):
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT stream_id, partition_id, offset, metadata FROM {self._table}"
                " WHERE stream_id = %s",
                (stream_id,),
            )
            rows = cur.fetchall()
        return [
            Checkpoint(
                stream_id=r[0],
                partition_id=r[1],
                offset=r[2],
                metadata=r[3] if r[3] else {},
            )
            for r in rows
        ]

    def close(self) -> None:
        self._conn.close()
