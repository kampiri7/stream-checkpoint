import json
import time
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class PlanetScaleTTLCheckpointStore(BaseCheckpointStore):
    """PlanetScale checkpoint store with TTL support."""

    def __init__(self, connection, table: str = "checkpoints", ttl: int = 3600):
        self._conn = connection
        self._table = table
        self._ttl = ttl
        self._create_table()

    def _create_table(self) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{self._table}` (
                `pipeline_id` VARCHAR(255) NOT NULL,
                `offset` TEXT NOT NULL,
                `metadata` TEXT,
                `timestamp` DOUBLE NOT NULL,
                `expires_at` DOUBLE NOT NULL,
                PRIMARY KEY (`pipeline_id`)
            )
            """
        )
        self._conn.commit()

    def save(self, checkpoint: Checkpoint) -> None:
        now = time.time()
        expires_at = now + self._ttl
        cursor = self._conn.cursor()
        cursor.execute(
            f"""
            INSERT INTO `{self._table}` (`pipeline_id`, `offset`, `metadata`, `timestamp`, `expires_at`)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `offset` = VALUES(`offset`),
                `metadata` = VALUES(`metadata`),
                `timestamp` = VALUES(`timestamp`),
                `expires_at` = VALUES(`expires_at`)
            """,
            (
                checkpoint.pipeline_id,
                json.dumps(checkpoint.offset),
                json.dumps(checkpoint.metadata) if checkpoint.metadata else None,
                checkpoint.timestamp,
                expires_at,
            ),
        )
        self._conn.commit()

    def load(self, pipeline_id: str) -> Optional[Checkpoint]:
        cursor = self._conn.cursor()
        cursor.execute(
            f"SELECT `offset`, `metadata`, `timestamp` FROM `{self._table}` "
            f"WHERE `pipeline_id` = %s AND `expires_at` > %s",
            (pipeline_id, time.time()),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return Checkpoint(
            pipeline_id=pipeline_id,
            offset=json.loads(row[0]),
            metadata=json.loads(row[1]) if row[1] else None,
            timestamp=row[2],
        )

    def delete(self, pipeline_id: str) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            f"DELETE FROM `{self._table}` WHERE `pipeline_id` = %s",
            (pipeline_id,),
        )
        self._conn.commit()

    def list_checkpoints(self):
        cursor = self._conn.cursor()
        cursor.execute(
            f"SELECT `pipeline_id`, `offset`, `metadata`, `timestamp` FROM `{self._table}` "
            f"WHERE `expires_at` > %s",
            (time.time(),),
        )
        rows = cursor.fetchall()
        return [
            Checkpoint(
                pipeline_id=r[0],
                offset=json.loads(r[1]),
                metadata=json.loads(r[2]) if r[2] else None,
                timestamp=r[3],
            )
            for r in rows
        ]
