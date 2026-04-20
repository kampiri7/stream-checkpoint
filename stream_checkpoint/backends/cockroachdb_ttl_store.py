import json
import time
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class CockroachDBTTLCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by CockroachDB with TTL-based expiration.

    Uses CockroachDB's native row-level TTL feature to automatically
    expire stale checkpoints.
    """

    def __init__(
        self,
        conn,
        table: str = "checkpoints",
        ttl_seconds: int = 86400,
        prefix: str = "",
    ):
        self._conn = conn
        self._table = table
        self._ttl_seconds = ttl_seconds
        self._prefix = prefix
        self._create_table()

    def _create_table(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._table} (
                    key TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '{self._ttl_seconds} seconds'
                ) WITH (ttl_expiration_expression = 'expires_at');
                """
            )
        self._conn.commit()

    def save(self, checkpoint: Checkpoint) -> None:
        key = f"{self._prefix}{checkpoint.stream_id}:{checkpoint.partition}"
        data = json.dumps(checkpoint.to_dict())
        with self._conn.cursor() as cur:
            cur.execute(
                f"""
                UPSERT INTO {self._table} (key, data, expires_at)
                VALUES (%s, %s, NOW() + INTERVAL '{self._ttl_seconds} seconds');
                """,
                (key, data),
            )
        self._conn.commit()

    def load(self, stream_id: str, partition: int) -> Optional[Checkpoint]:
        key = f"{self._prefix}{stream_id}:{partition}"
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT data FROM {self._table} WHERE key = %s AND expires_at > NOW();",
                (key,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return Checkpoint.from_dict(json.loads(row[0]))

    def delete(self, stream_id: str, partition: int) -> None:
        key = f"{self._prefix}{stream_id}:{partition}"
        with self._conn.cursor() as cur:
            cur.execute(f"DELETE FROM {self._table} WHERE key = %s;", (key,))
        self._conn.commit()

    def list_checkpoints(self, stream_id: str):
        prefix = f"{self._prefix}{stream_id}:"
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT data FROM {self._table} WHERE key LIKE %s AND expires_at > NOW();",
                (prefix + "%",),
            )
            rows = cur.fetchall()
        return [Checkpoint.from_dict(json.loads(row[0])) for row in rows]
