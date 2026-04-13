import json
from typing import Optional
from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class PlanetScaleCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by PlanetScale (MySQL-compatible serverless DB)."""

    def __init__(self, client, database: str, table: str = "checkpoints"):
        """
        Args:
            client: A PlanetScale/MySQL-compatible connection object.
            database: Name of the database to use.
            table: Table name for storing checkpoints.
        """
        self._conn = client
        self._database = database
        self._table = table
        self._create_table()

    def _create_table(self) -> None:
        cursor = self._conn.cursor()
        cursor.execute(f"USE `{self._database}`")
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{self._table}` (
                `stream_id` VARCHAR(255) NOT NULL,
                `partition` VARCHAR(255) NOT NULL,
                `offset` BIGINT NOT NULL,
                `metadata` JSON,
                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`stream_id`, `partition`)
            )
            """
        )
        self._conn.commit()

    def save(self, checkpoint: Checkpoint) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            f"""
            INSERT INTO `{self._table}` (`stream_id`, `partition`, `offset`, `metadata`)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `offset` = VALUES(`offset`),
                `metadata` = VALUES(`metadata`),
                `updated_at` = CURRENT_TIMESTAMP
            """,
            (
                checkpoint.stream_id,
                checkpoint.partition,
                checkpoint.offset,
                json.dumps(checkpoint.metadata) if checkpoint.metadata else None,
            ),
        )
        self._conn.commit()

    def load(self, stream_id: str, partition: str) -> Optional[Checkpoint]:
        cursor = self._conn.cursor()
        cursor.execute(
            f"SELECT `stream_id`, `partition`, `offset`, `metadata` FROM `{self._table}` "
            f"WHERE `stream_id` = %s AND `partition` = %s",
            (stream_id, partition),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        sid, part, offset, meta = row
        metadata = json.loads(meta) if meta else {}
        return Checkpoint(stream_id=sid, partition=part, offset=offset, metadata=metadata)

    def delete(self, stream_id: str, partition: str) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            f"DELETE FROM `{self._table}` WHERE `stream_id` = %s AND `partition` = %s",
            (stream_id, partition),
        )
        self._conn.commit()

    def list_checkpoints(self, stream_id: str):
        cursor = self._conn.cursor()
        cursor.execute(
            f"SELECT `stream_id`, `partition`, `offset`, `metadata` FROM `{self._table}` "
            f"WHERE `stream_id` = %s",
            (stream_id,),
        )
        rows = cursor.fetchall()
        result = []
        for sid, part, offset, meta in rows:
            metadata = json.loads(meta) if meta else {}
            result.append(Checkpoint(stream_id=sid, partition=part, offset=offset, metadata=metadata))
        return result
