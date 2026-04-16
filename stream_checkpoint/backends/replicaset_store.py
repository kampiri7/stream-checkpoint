"""MongoDB Replica Set checkpoint store backend."""

import json
from datetime import datetime, timezone
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class MongoDBReplicaSetCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by a MongoDB Replica Set for high availability."""

    def __init__(
        self,
        client,
        database: str = "checkpoints",
        collection: str = "checkpoints",
        read_preference=None,
    ):
        self._client = client
        self._collection = client[database][collection]
        if read_preference is not None:
            self._collection = self._collection.with_options(
                read_preference=read_preference
            )
        self._collection.create_index(
            [("pipeline_id", 1), ("stream_id", 1)], unique=True
        )

    def _filter(self, pipeline_id: str, stream_id: str) -> dict:
        return {"pipeline_id": pipeline_id, "stream_id": stream_id}

    def save(self, checkpoint: Checkpoint) -> None:
        doc = checkpoint.to_dict()
        self._collection.update_one(
            self._filter(checkpoint.pipeline_id, checkpoint.stream_id),
            {"$set": doc},
            upsert=True,
        )

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        doc = self._collection.find_one(
            self._filter(pipeline_id, stream_id), {"_id": 0}
        )
        if doc is None:
            return None
        return Checkpoint.from_dict(doc)

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        self._collection.delete_one(self._filter(pipeline_id, stream_id))

    def list_checkpoints(self, pipeline_id: str):
        docs = self._collection.find({"pipeline_id": pipeline_id}, {"_id": 0})
        return [Checkpoint.from_dict(d) for d in docs]

    def close(self) -> None:
        self._client.close()
