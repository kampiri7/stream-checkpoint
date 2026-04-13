import json
from typing import Optional
from stream_checkpoint.base import Checkpoint, BaseCheckpointStore


class WeaviateCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by Weaviate vector database.

    Uses a dedicated Weaviate class (collection) to store checkpoint documents
    keyed by pipeline_id + stream_id.
    """

    DEFAULT_CLASS = "StreamCheckpoint"

    def __init__(self, client, class_name: str = DEFAULT_CLASS):
        """
        :param client: A weaviate.Client instance.
        :param class_name: Weaviate class name used to store checkpoints.
        """
        self._client = client
        self._class_name = class_name
        self._ensure_class()

    def _ensure_class(self) -> None:
        """Create the Weaviate class schema if it does not already exist."""
        existing = self._client.schema.get()
        class_names = [c["class"] for c in existing.get("classes", [])]
        if self._class_name not in class_names:
            self._client.schema.create_class(
                {
                    "class": self._class_name,
                    "properties": [
                        {"name": "checkpoint_key", "dataType": ["text"]},
                        {"name": "payload", "dataType": ["text"]},
                    ],
                }
            )

    def _doc_id(self, pipeline_id: str, stream_id: str) -> str:
        return f"{pipeline_id}::{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._doc_id(checkpoint.pipeline_id, checkpoint.stream_id)
        payload = json.dumps(checkpoint.to_dict())
        # Delete existing object for this key before upserting
        self.delete(checkpoint.pipeline_id, checkpoint.stream_id)
        self._client.data_object.create(
            data_object={"checkpoint_key": key, "payload": payload},
            class_name=self._class_name,
        )

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._doc_id(pipeline_id, stream_id)
        result = (
            self._client.query
            .get(self._class_name, ["checkpoint_key", "payload", "_additional {id}"])
            .with_where(
                {
                    "path": ["checkpoint_key"],
                    "operator": "Equal",
                    "valueText": key,
                }
            )
            .with_limit(1)
            .do()
        )
        objects = (
            result.get("data", {}).get("Get", {}).get(self._class_name, [])
        )
        if not objects:
            return None
        return Checkpoint.from_dict(json.loads(objects[0]["payload"]))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._doc_id(pipeline_id, stream_id)
        result = (
            self._client.query
            .get(self._class_name, ["_additional {id}"])
            .with_where(
                {
                    "path": ["checkpoint_key"],
                    "operator": "Equal",
                    "valueText": key,
                }
            )
            .with_limit(100)
            .do()
        )
        objects = (
            result.get("data", {}).get("Get", {}).get(self._class_name, [])
        )
        for obj in objects:
            self._client.data_object.delete(
                obj["_additional"]["id"], class_name=self._class_name
            )

    def list_checkpoints(self, pipeline_id: str):
        result = (
            self._client.query
            .get(self._class_name, ["checkpoint_key", "payload"])
            .with_where(
                {
                    "path": ["checkpoint_key"],
                    "operator": "Like",
                    "valueText": f"{pipeline_id}::*",
                }
            )
            .with_limit(1000)
            .do()
        )
        objects = (
            result.get("data", {}).get("Get", {}).get(self._class_name, [])
        )
        return [Checkpoint.from_dict(json.loads(o["payload"])) for o in objects]
