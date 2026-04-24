import json
from ..base import BaseCheckpointStore, Checkpoint


class AppwriteCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Appwrite Databases."""

    def __init__(self, client, database_id: str, collection_id: str, prefix: str = "checkpoint"):
        """
        Args:
            client: An Appwrite ``Databases`` service instance.
            database_id: The Appwrite database ID.
            collection_id: The Appwrite collection ID.
            prefix: Optional key prefix used to namespace document IDs.
        """
        self._db = client
        self._database_id = database_id
        self._collection_id = collection_id
        self._prefix = prefix

    def _doc_id(self, pipeline_id: str, key: str) -> str:
        return f"{self._prefix}_{pipeline_id}_{key}"

    def save(self, checkpoint: Checkpoint) -> None:
        doc_id = self._doc_id(checkpoint.pipeline_id, checkpoint.key)
        data = checkpoint.to_dict()
        data["payload"] = json.dumps(data.get("payload", {}))
        try:
            self._db.get_document(
                database_id=self._database_id,
                collection_id=self._collection_id,
                document_id=doc_id,
            )
            self._db.update_document(
                database_id=self._database_id,
                collection_id=self._collection_id,
                document_id=doc_id,
                data=data,
            )
        except Exception:
            self._db.create_document(
                database_id=self._database_id,
                collection_id=self._collection_id,
                document_id=doc_id,
                data=data,
            )

    def load(self, pipeline_id: str, key: str) -> Checkpoint | None:
        doc_id = self._doc_id(pipeline_id, key)
        try:
            doc = self._db.get_document(
                database_id=self._database_id,
                collection_id=self._collection_id,
                document_id=doc_id,
            )
            raw = dict(doc)
            raw["payload"] = json.loads(raw.get("payload", "{}"))
            return Checkpoint.from_dict(raw)
        except Exception:
            return None

    def delete(self, pipeline_id: str, key: str) -> None:
        doc_id = self._doc_id(pipeline_id, key)
        try:
            self._db.delete_document(
                database_id=self._database_id,
                collection_id=self._collection_id,
                document_id=doc_id,
            )
        except Exception:
            pass

    def list_checkpoints(self, pipeline_id: str) -> list[Checkpoint]:
        prefix = f"{self._prefix}_{pipeline_id}_"
        result = self._db.list_documents(
            database_id=self._database_id,
            collection_id=self._collection_id,
        )
        checkpoints = []
        for doc in result.get("documents", []):
            if doc.get("$id", "").startswith(prefix):
                raw = dict(doc)
                raw["payload"] = json.loads(raw.get("payload", "{}"))
                checkpoints.append(Checkpoint.from_dict(raw))
        return checkpoints
