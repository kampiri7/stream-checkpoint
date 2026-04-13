import json
from datetime import datetime, timezone
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class RethinkDBCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by RethinkDB.

    Args:
        conn: A RethinkDB connection object (from ``rethinkdb.r.connect()``).
        db: Database name to use.
        table: Table name to use for checkpoints.
    """

    def __init__(self, conn, db: str = "stream_checkpoint", table: str = "checkpoints"):
        self._conn = conn
        self._db = db
        self._table = table
        self._r = None  # lazily imported
        self._ensure_table()

    def _get_r(self):
        if self._r is None:
            import rethinkdb as r
            self._r = r.RethinkDB()
        return self._r

    def _ensure_table(self):
        r = self._get_r()
        existing_dbs = r.db_list().run(self._conn)
        if self._db not in existing_dbs:
            r.db_create(self._db).run(self._conn)
        existing_tables = r.db(self._db).table_list().run(self._conn)
        if self._table not in existing_tables:
            r.db(self._db).table_create(self._table, primary_key="checkpoint_id").run(self._conn)

    def save(self, checkpoint: Checkpoint) -> None:
        r = self._get_r()
        doc = checkpoint.to_dict()
        doc["checkpoint_id"] = checkpoint.checkpoint_id
        r.db(self._db).table(self._table).insert(doc, conflict="replace").run(self._conn)

    def load(self, checkpoint_id: str) -> Optional[Checkpoint]:
        r = self._get_r()
        doc = r.db(self._db).table(self._table).get(checkpoint_id).run(self._conn)
        if doc is None:
            return None
        return Checkpoint.from_dict(doc)

    def delete(self, checkpoint_id: str) -> None:
        r = self._get_r()
        r.db(self._db).table(self._table).get(checkpoint_id).delete().run(self._conn)

    def list_checkpoints(self, pipeline_id: str) -> list:
        r = self._get_r()
        cursor = (
            r.db(self._db)
            .table(self._table)
            .filter({"pipeline_id": pipeline_id})
            .run(self._conn)
        )
        return [Checkpoint.from_dict(doc) for doc in cursor]
