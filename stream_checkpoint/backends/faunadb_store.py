import json
from typing import Optional

from stream_checkpoint.base import Checkpoint, BaseCheckpointStore


class FaunaDBCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by FaunaDB (Fauna).

    Uses a Fauna collection to persist checkpoints.  Each checkpoint is stored
    as a document whose ``data`` field contains the serialised checkpoint dict.
    Documents are indexed by ``pipeline_id`` so that lookups are O(log n).

    Args:
        client: A ``fauna.Client`` (or compatible) instance.
        collection: Name of the Fauna collection to use (default: ``"checkpoints"``).
        index: Name of the Fauna index used to query by pipeline_id
               (default: ``"checkpoints_by_pipeline_id"``).
    """

    def __init__(
        self,
        client,
        collection: str = "checkpoints",
        index: str = "checkpoints_by_pipeline_id",
    ) -> None:
        self._client = client
        self._collection = collection
        self._index = index

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _query(self, fql):
        """Execute a raw FQL query via the injected client."""
        return self._client.query(fql)

    # ------------------------------------------------------------------
    # BaseCheckpointStore interface
    # ------------------------------------------------------------------

    def save(self, checkpoint: Checkpoint) -> None:
        data = checkpoint.to_dict()
        pipeline_id = data["pipeline_id"]
        existing = self.load(pipeline_id)
        if existing is None:
            self._query(
                {
                    "create": {"collection": self._collection},
                    "params": {"data": data},
                }
            )
        else:
            ref = self._client.find_ref(self._index, pipeline_id)
            self._query({"update": ref, "params": {"data": data}})

    def load(self, pipeline_id: str) -> Optional[Checkpoint]:
        result = self._client.find_one(self._index, pipeline_id)
        if result is None:
            return None
        return Checkpoint.from_dict(result["data"])

    def delete(self, pipeline_id: str) -> None:
        ref = self._client.find_ref(self._index, pipeline_id)
        if ref is not None:
            self._query({"delete": ref})

    def list_checkpoints(self):
        results = self._client.list_all(self._collection)
        return [Checkpoint.from_dict(r["data"]) for r in results]
