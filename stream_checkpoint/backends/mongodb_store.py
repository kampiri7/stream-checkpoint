import json
from datetime import datetime, timezone
from typing import List, Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class MongoDBCheckpointStore(BaseCheckpointStore):
    """
    MongoDB-backed checkpoint store.

    Requires pymongo to be installed.
    Documents are stored in the given collection with a compound
    unique index on (pipeline_id, stream_id).
    """

    def __init__(
        self,
        collection,
    ):
        """
        :param collection: A pymongo Collection instance (or compatible stub).
        """
        self._col = collection
        self._col.create_index(
            [("pipeline_id", 1), ("stream_id", 1)], unique=True
        )

    def _filter(self, pipeline_id: str, stream_id: str) -> dict:
        return {"pipeline_id": pipeline_id, "stream_id": stream_id}

    def save(self, checkpoint: Checkpoint) -> None:
        doc = {
            "pipeline_id": checkpoint.pipeline_id,
            "stream_id": checkpoint.stream_id,
            "offset": checkpoint.offset,
            "metadata": checkpoint.metadata or {},
            "updated_at": checkpoint.updated_at.isoformat(),
        }
        self._col.update_one(
            self._filter(checkpoint.pipeline_id, checkpoint.stream_id),
            {"$set": doc},
            upsert=True,
        )

    def load(
        self, pipeline_id: str, stream_id: str
    ) -> Optional[Checkpoint]:
        doc = self._col.find_one(self._filter(pipeline_id, stream_id))
        if doc is None:
            return None
        return Checkpoint(
            pipeline_id=doc["pipeline_id"],
            stream_id=doc["stream_id"],
            offset=doc["offset"],
            metadata=doc.get("metadata", {}),
            updated_at=datetime.fromisoformat(doc["updated_at"]),
        )

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        self._col.delete_one(self._filter(pipeline_id, stream_id))

    def list_checkpoints(self, pipeline_id: str) -> List[Checkpoint]:
        cursor = self._col.find({"pipeline_id": pipeline_id})
        results = []
        for doc in cursor:
            results.append(
                Checkpoint(
                    pipeline_id=doc["pipeline_id"],
                    stream_id=doc["stream_id"],
                    offset=doc["offset"],
                    metadata=doc.get("metadata", {}),
                    updated_at=datetime.fromisoformat(doc["updated_at"]),
                )
            )
        return results
