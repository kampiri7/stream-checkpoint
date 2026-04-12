"""CockroachDB checkpoint store backend."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class CockroachDBCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by CockroachDB (PostgreSQL-compatible).

    Args:
        conn: A psycopg2-compatible connection to CockroachDB.
        table: Table name used to store checkpoints (default: ``checkpoints``).
    """

    def __init__(self, conn, table: str = "checkpoints") -> None:
        self._conn = conn
        self._table = table
        self._create_table()

    def _create_table(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._table} (
                    stream_id  TEXT        NOT NULL,
                    key        TEXT        NOT NULL,
                    data       TEXT        NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (stream_id, key)
                )
                """
            )
        self._conn.commit()

    def save(self, checkpoint: Checkpoint) -> None:
        data = json.dumps(checkpoint.to_dict())
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {self._table} (stream_id, key, data, updated_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (stream_id, key)
                DO UPDATE SET data = EXCLUDED.data, updated_at = now()
                """,
                (checkpoint.stream_id, checkpoint.key, data),
            )
        self._conn.commit()

    def load(self, stream_id: str, key: str) -> Optional[Checkpoint]:
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT data FROM {self._table} WHERE stream_id = %s AND key = %s",
                (stream_id, key),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return Checkpoint.from_dict(json.loads(row[0]))

    def delete(self, stream_id: str, key: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self._table} WHERE stream_id = %s AND key = %s",
                (stream_id, key),
            )
        self._conn.commit()

    def list_keys(self, stream_id: str):
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT key FROM {self._table} WHERE stream_id = %s ORDER BY key",
                (stream_id,),
            )
            rows = cur.fetchall()
        return [r[0] for r in rows]
