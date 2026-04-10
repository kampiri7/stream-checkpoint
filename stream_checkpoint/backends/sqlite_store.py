import json
import sqlite3
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class SQLiteCheckpointStore(BaseCheckpointStore):
    """SQLite-backed checkpoint store for persistent local storage."""

    DEFAULT_TABLE = "checkpoints"

    def __init__(self, db_path: str, table: str = DEFAULT_TABLE) -> None:
        """
        Initialize the SQLite checkpoint store.

        Args:
            db_path: Path to the SQLite database file. Use ':memory:' for in-memory.
            table: Name of the table to store checkpoints in.
        """
        self.db_path = db_path
        self.table = table
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._create_table()

    def _create_table(self) -> None:
        """Create the checkpoints table if it does not exist."""
        self._conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                pipeline_id TEXT NOT NULL,
                stream_id   TEXT NOT NULL,
                data        TEXT NOT NULL,
                PRIMARY KEY (pipeline_id, stream_id)
            )
            """
        )
        self._conn.commit()

    def save(self, checkpoint: Checkpoint) -> None:
        """Persist a checkpoint, replacing any existing entry."""
        payload = json.dumps(checkpoint.to_dict())
        self._conn.execute(
            f"""
            INSERT INTO {self.table} (pipeline_id, stream_id, data)
            VALUES (?, ?, ?)
            ON CONFLICT(pipeline_id, stream_id) DO UPDATE SET data = excluded.data
            """,
            (checkpoint.pipeline_id, checkpoint.stream_id, payload),
        )
        self._conn.commit()

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        """Load a checkpoint by pipeline and stream identifiers."""
        cursor = self._conn.execute(
            f"SELECT data FROM {self.table} WHERE pipeline_id = ? AND stream_id = ?",
            (pipeline_id, stream_id),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return Checkpoint.from_dict(json.loads(row[0]))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        """Remove a checkpoint from the store."""
        self._conn.execute(
            f"DELETE FROM {self.table} WHERE pipeline_id = ? AND stream_id = ?",
            (pipeline_id, stream_id),
        )
        self._conn.commit()

    def list_checkpoints(self, pipeline_id: str) -> list:
        """Return all checkpoints for a given pipeline."""
        cursor = self._conn.execute(
            f"SELECT data FROM {self.table} WHERE pipeline_id = ?",
            (pipeline_id,),
        )
        return [Checkpoint.from_dict(json.loads(row[0])) for row in cursor.fetchall()]

    def close(self) -> None:
        """Close the underlying database connection."""
        self._conn.close()
