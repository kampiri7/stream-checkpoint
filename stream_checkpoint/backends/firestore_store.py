import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class FirestoreCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by Google Cloud Firestore.

    Each checkpoint is stored as a document in a Firestore collection.
    The document ID is derived from the pipeline_id and stream_id.
    """

    def __init__(self, client, collection: str = "stream_checkpoints"):
        """
        :param client: A ``google.cloud.firestore.Client`` instance.
        :param collection: Firestore collection name to store checkpoints in.
        """
        self._client = client
        self._collection = collection

    def _doc_id(self, pipeline_id: str, stream_id: str) -> str:
        return f"{pipeline_id}::{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        doc_id = self._doc_id(checkpoint.pipeline_id, checkpoint.stream_id)
        doc_ref = self._client.collection(self._collection).document(doc_id)
        doc_ref.set(checkpoint.to_dict())

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        doc_id = self._doc_id(pipeline_id, stream_id)
        doc_ref = self._client.collection(self._collection).document(doc_id)
        snapshot = doc_ref.get()
        if not snapshot.exists:
            return None
        return Checkpoint.from_dict(snapshot.to_dict())

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        doc_id = self._doc_id(pipeline_id, stream_id)
        self._client.collection(self._collection).document(doc_id).delete()

    def list_checkpoints(self, pipeline_id: str) -> list:
        docs = self._client.collection(self._collection).stream()
        results = []
        for doc in docs:
            data = doc.to_dict()
            if data.get("pipeline_id") == pipeline_id:
                results.append(Checkpoint.from_dict(data))
        return results
