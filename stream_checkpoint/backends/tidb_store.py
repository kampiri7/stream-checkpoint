import json
from typing import Optional
from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class TiDBCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by TiDB (MySQL-compatible distributed SQL database).

    Parameters
    ----------
    connection:
        A PEP 249-compatible database connection (e.g. from ``mysql-connector-python``).
    table : str
        Name of the table used to persist checkpoints. Created automatically
        if it does not exist.
    """

    def __init__(self, connection, table: str = "checkpoints") -> None:
        self._conn = connection
        self._table = table
        self._create_table()

    def _create_table(self) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{self._table}` (
                pipeline_id VARCHAR(255) NOT NULL,
                stream_id   VARCHAR(255) NOT NULL,
                offset      BIGINT       NOT NULL,
                metadata    TEXT,
                updated_at  DATETIME     NOT NULL,
                PRIMARY KEY (pipeline_id, stream_id)
            )
            """
        )
        self._conn.commit()

    def save(self, checkpoint: Checkpoint) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            f"""
            INSERT INTO `{self._table}`
                (pipeline_id, stream_id, offset, metadata, updated_at)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                offset     = VALUES(offset),
                metadata   = VALUES(metadata),
                updated_at = VALUES(updated_at)
            """,
            (
                checkpoint.pipeline_id,
                checkpoint.stream_id,
                checkpoint.offset,
                json.dumps(checkpoint.metadata) if checkpoint.metadata is not None else None,
                checkpoint.updated_at,
            ),
        )
        self._conn.commit()

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        cursor = self._conn.cursor()
        cursor.execute(
            f"SELECT pipeline_id, stream_id, offset, metadata, updated_at "
            f"FROM `{self._table}` WHERE pipeline_id = %s AND stream_id = %s",
            (pipeline_id, stream_id),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        pid, sid, offset, metadata_raw, updated_at = row
        return Checkpoint(
            pipeline_id=pid,
            stream_id=sid,
            offset=offset,
            metadata=json.loads(metadata_raw) if metadata_raw else None,
            updated_at=updated_at,
        )

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            f"DELETE FROM `{self._table}` WHERE pipeline_id = %s AND stream_id = %s",
            (pipeline_id, stream_id),
        )
        self._conn.commit()

    def list_checkpoints(self, pipeline_id: str):
        cursor = self._conn.cursor()
        cursor.execute(
            f"SELECT pipeline_id, stream_id, offset, metadata, updated_at "
            f"FROM `{self._table}` WHERE pipeline_id = %s",
            (pipeline_id,),
        )
        rows = cursor.fetchall()
        return [
            Checkpoint(
                pipeline_id=r[0],
                stream_id=r[1],
                offset=r[2],
                metadata=json.loads(r[3]) if r[3] else None,
                updated_at=r[4],
            )
            for r in rows
        ]
