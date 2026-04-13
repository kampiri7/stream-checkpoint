import json
from datetime import datetime, timezone
from stream_checkpoint.base import Checkpoint, BaseCheckpointStore


class ArangoDBCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by ArangoDB.

    Requires the ``python-arango`` package::

        pip install python-arango
    """

    def __init__(self, client, database: str, collection: str = "checkpoints"):
        """
        :param client: An ``ArangoClient`` instance (python-arango).
        :param database: Name of the ArangoDB database to use.
        :param collection: Name of the collection to store checkpoints in.
        """
        db = client.db(database)
        if not db.has_collection(collection):
            db.create_collection(collection)
        self._col = db.collection(collection)

    def _doc_key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{pipeline_id}::{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._doc_key(checkpoint.pipeline_id, checkpoint.stream_id)
        doc = checkpoint.to_dict()
        doc["_key"] = key
        self._col.insert(doc, overwrite=True)

    def load(self, pipeline_id: str, stream_id: str):
        key = self._doc_key(pipeline_id, stream_id)
        try:
            doc = self._col.get(key)
        except Exception:
            return None
        if doc is None:
            return None
        doc.pop("_key", None)
        doc.pop("_id", None)
        doc.pop("_rev", None)
        return Checkpoint.from_dict(doc)

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._doc_key(pipeline_id, stream_id)
        try:
            self._col.delete(key, ignore_missing=True)
        except Exception:
            pass

    def list_checkpoints(self, pipeline_id: str):
        cursor = self._col.find({"pipeline_id": pipeline_id})
        results = []
        for doc in cursor:
            doc.pop("_key", None)
            doc.pop("_id", None)
            doc.pop("_rev", None)
            results.append(Checkpoint.from_dict(doc))
        return results

    def exists(self, pipeline_id: str, stream_id: str) -> bool:
        return self.load(pipeline_id, stream_id) is not None
