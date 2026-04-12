"""Google Cloud Spanner checkpoint store backend."""

import json
from datetime import datetime, timezone
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class SpannerCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Google Cloud Spanner."""

    def __init__(
        self,
        instance_id: str,
        database_id: str,
        table: str = "checkpoints",
        client=None,
    ):
        if client is None:
            from google.cloud import spanner  # type: ignore
            client = spanner.Client()
        self._instance = client.instance(instance_id)
        self._database = self._instance.database(database_id)
        self._table = table
        self._ensure_table()

    def _ensure_table(self) -> None:
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {self._table} ("
            "pipeline_id STRING(256) NOT NULL,"
            "stream_id STRING(256) NOT NULL,"
            "offset STRING(MAX) NOT NULL,"
            "metadata STRING(MAX),"
            "updated_at TIMESTAMP NOT NULL"
            ") PRIMARY KEY (pipeline_id, stream_id)"
        )
        op = self._database.update_ddl([ddl])
        op.result()

    def save(self, checkpoint: Checkpoint) -> None:
        with self._database.batch() as batch:
            batch.insert_or_update(
                table=self._table,
                columns=("pipeline_id", "stream_id", "offset", "metadata", "updated_at"),
                values=[
                    (
                        checkpoint.pipeline_id,
                        checkpoint.stream_id,
                        str(checkpoint.offset),
                        json.dumps(checkpoint.metadata or {}),
                        checkpoint.updated_at or datetime.now(timezone.utc),
                    )
                ],
            )

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        with self._database.snapshot() as snapshot:
            keyset = self._database.__class__.__module__  # avoid import at top level
            from google.cloud.spanner_v1 import KeySet  # type: ignore
            results = snapshot.read(
                table=self._table,
                columns=("pipeline_id", "stream_id", "offset", "metadata", "updated_at"),
                keyset=KeySet(keys=[[pipeline_id, stream_id]]),
            )
            row = next(iter(results), None)
            if row is None:
                return None
            return Checkpoint(
                pipeline_id=row[0],
                stream_id=row[1],
                offset=row[2],
                metadata=json.loads(row[3]) if row[3] else {},
                updated_at=row[4],
            )

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        from google.cloud.spanner_v1 import KeySet  # type: ignore
        with self._database.batch() as batch:
            batch.delete(self._table, KeySet(keys=[[pipeline_id, stream_id]]))

    def list_checkpoints(self, pipeline_id: str):
        with self._database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                f"SELECT pipeline_id, stream_id, offset, metadata, updated_at "
                f"FROM {self._table} WHERE pipeline_id = @pid",
                params={"pid": pipeline_id},
                param_types={"pid": self._database.__class__.__module__},
            )
            return [
                Checkpoint(
                    pipeline_id=r[0],
                    stream_id=r[1],
                    offset=r[2],
                    metadata=json.loads(r[3]) if r[3] else {},
                    updated_at=r[4],
                )
                for r in results
            ]
