"""Supabase (PostgreSQL-based) checkpoint store backend."""

import json
from datetime import datetime, timezone
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class SupabaseCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Supabase (via psycopg2-compatible connection).

    Args:
        client: A Supabase Python client instance (``supabase.Client``).
        table: Name of the table used to store checkpoints.
    """

    def __init__(self, client, table: str = "checkpoints") -> None:
        self._client = client
        self._table = table
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create the checkpoints table if it does not already exist."""
        self._client.postgrest.schema("public")
        # Supabase REST API does not support DDL; rely on the user having
        # created the table, or use the management API.  For testability we
        # expose a hook but skip silently when the table already exists.
        try:
            self._client.table(self._table).select("pipeline_id").limit(1).execute()
        except Exception:
            pass  # Table may not exist yet; creation is the user's responsibility.

    def save(self, checkpoint: Checkpoint) -> None:
        data = {
            "pipeline_id": checkpoint.pipeline_id,
            "offset": json.dumps(checkpoint.offset),
            "metadata": json.dumps(checkpoint.metadata or {}),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._client.table(self._table).upsert(
            data, on_conflict="pipeline_id"
        ).execute()

    def load(self, pipeline_id: str) -> Optional[Checkpoint]:
        response = (
            self._client.table(self._table)
            .select("*")
            .eq("pipeline_id", pipeline_id)
            .limit(1)
            .execute()
        )
        rows = response.data
        if not rows:
            return None
        row = rows[0]
        return Checkpoint(
            pipeline_id=row["pipeline_id"],
            offset=json.loads(row["offset"]),
            metadata=json.loads(row["metadata"]),
        )

    def delete(self, pipeline_id: str) -> None:
        self._client.table(self._table).delete().eq(
            "pipeline_id", pipeline_id
        ).execute()

    def list_checkpoints(self) -> list:
        response = self._client.table(self._table).select("*").execute()
        return [
            Checkpoint(
                pipeline_id=row["pipeline_id"],
                offset=json.loads(row["offset"]),
                metadata=json.loads(row["metadata"]),
            )
            for row in response.data
        ]
