import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class CouchDBCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by Apache CouchDB.

    Each checkpoint is stored as a CouchDB document in the specified
    database. The document ID is derived from the pipeline and stream IDs.
    """

    def __init__(self, client, database: str = "checkpoints"):
        """
        :param client: A CouchDB server/session object (e.g. from ``couchdb`` library).
        :param database: Name of the CouchDB database to use.
        """
        self._client = client
        self._db_name = database
        self._db = self._get_or_create_db()

    def _get_or_create_db(self):
        try:
            return self._client[self._db_name]
        except Exception:
            return self._client.create(self._db_name)

    def _doc_id(self, pipeline_id: str, stream_id: str) -> str:
        return f"{pipeline_id}::{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        doc_id = self._doc_id(checkpoint.pipeline_id, checkpoint.stream_id)
        data = checkpoint.to_dict()
        data["_id"] = doc_id
        try:
            existing = self._db[doc_id]
            data["_rev"] = existing["_rev"]
        except Exception:
            pass
        self._db.save(data)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        doc_id = self._doc_id(pipeline_id, stream_id)
        try:
            doc = self._db[doc_id]
            clean = {k: v for k, v in doc.items() if not k.startswith("_")}
            return Checkpoint.from_dict(clean)
        except Exception:
            return None

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        doc_id = self._doc_id(pipeline_id, stream_id)
        try:
            doc = self._db[doc_id]
            self._db.delete(doc)
        except Exception:
            pass

    def list_checkpoints(self, pipeline_id: str):
        prefix = f"{pipeline_id}::"
        results = []
        for doc_id in self._db:
            if doc_id.startswith(prefix):
                doc = self._db[doc_id]
                clean = {k: v for k, v in doc.items() if not k.startswith("_")}
                results.append(Checkpoint.from_dict(clean))
        return results
