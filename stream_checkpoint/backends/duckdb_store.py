import json
import time
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class DuckDBCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by DuckDB."""

    def __init__(self, database: str = ":memory:", table: str = "checkpoints"):
        """
        Args:
            database: Path to DuckDB database file, or ':memory:' for in-memory.
            table: Table name to store checkpoints in.
        """
        import duckdb

        self._conn = duckdb.connect(database)
        self._table = table
        self._create_table()

    def _create_table(self) -> None:
        self._conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table} (
                stream_id  VARCHAR NOT NULL,
                pipeline   VARCHAR NOT NULL,
                offset     VARCHAR NOT NULL,
                metadata   VARCHAR,
                updated_at DOUBLE NOT NULL,
                PRIMARY KEY (stream_id, pipeline)
            )
            """
        )

    def save(self, checkpoint: Checkpoint) -> None:
        meta = json.dumps(checkpoint.metadata) if checkpoint.metadata is not None else None
        self._conn.execute(
            f"""
            INSERT INTO {self._table} (stream_id, pipeline, offset, metadata, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (stream_id, pipeline) DO UPDATE SET
                offset     = excluded.offset,
                metadata   = excluded.metadata,
                updated_at = excluded.updated_at
            """,
            [checkpoint.stream_id, checkpoint.pipeline, checkpoint.offset, meta, time.time()],
        )

    def load(self, stream_id: str, pipeline: str) -> Optional[Checkpoint]:
        result = self._conn.execute(
            f"SELECT stream_id, pipeline, offset, metadata FROM {self._table} "
            f"WHERE stream_id = ? AND pipeline = ?",
            [stream_id, pipeline],
        ).fetchone()
        if result is None:
            return None
        meta = json.loads(result[3]) if result[3] is not None else None
        return Checkpoint(stream_id=result[0], pipeline=result[1], offset=result[2], metadata=meta)

    def delete(self, stream_id: str, pipeline: str) -> None:
        self._conn.execute(
            f"DELETE FROM {self._table} WHERE stream_id = ? AND pipeline = ?",
            [stream_id, pipeline],
        )

    def list_checkpoints(self, pipeline: str) -> list:
        rows = self._conn.execute(
            f"SELECT stream_id, pipeline, offset, metadata FROM {self._table} "
            f"WHERE pipeline = ?",
            [pipeline],
        ).fetchall()
        return [
            Checkpoint(
                stream_id=r[0],
                pipeline=r[1],
                offset=r[2],
                metadata=json.loads(r[3]) if r[3] is not None else None,
            )
            for r in rows
        ]

    def close(self) -> None:
        self._conn.close()
