import json
from stream_checkpoint.base import Checkpoint, BaseCheckpointStore


class PocketBaseCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by PocketBase REST API.

    :param client: A PocketBase HTTP client with ``collection(name)`` support.
    :param collection: PocketBase collection name (default: ``"checkpoints"``).
    :param prefix: Optional key prefix applied to every checkpoint id.
    """

    def __init__(self, client, collection: str = "checkpoints", prefix: str = ""):
        self._client = client
        self._collection = collection
        self._prefix = prefix

    def _doc_id(self, pipeline_id: str, source_id: str) -> str:
        raw = f"{pipeline_id}:{source_id}"
        if self._prefix:
            raw = f"{self._prefix}:{raw}"
        # PocketBase record IDs must be alphanumeric; replace separators.
        return raw.replace(":", "__").replace("/", "_")

    def save(self, checkpoint: Checkpoint) -> None:
        doc_id = self._doc_id(checkpoint.pipeline_id, checkpoint.source_id)
        data = {"doc_id": doc_id, "payload": json.dumps(checkpoint.to_dict())}
        col = self._client.collection(self._collection)
        try:
            existing = col.get_first_list_item(f'doc_id="{doc_id}"')
            col.update(existing.id, data)
        except Exception:
            col.create(data)

    def load(self, pipeline_id: str, source_id: str):
        doc_id = self._doc_id(pipeline_id, source_id)
        col = self._client.collection(self._collection)
        try:
            record = col.get_first_list_item(f'doc_id="{doc_id}"')
            return Checkpoint.from_dict(json.loads(record.payload))
        except Exception:
            return None

    def delete(self, pipeline_id: str, source_id: str) -> None:
        doc_id = self._doc_id(pipeline_id, source_id)
        col = self._client.collection(self._collection)
        try:
            record = col.get_first_list_item(f'doc_id="{doc_id}"')
            col.delete(record.id)
        except Exception:
            pass

    def list_checkpoints(self, pipeline_id: str):
        prefix = self._doc_id(pipeline_id, "")
        col = self._client.collection(self._collection)
        records = col.get_full_list(query_params={"filter": f'doc_id~"{prefix}"'})
        return [Checkpoint.from_dict(json.loads(r.payload)) for r in records]
