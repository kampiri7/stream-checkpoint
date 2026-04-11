"""DynamoDB checkpoint store with TTL (Time-To-Live) support."""

import json
import time
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class DynamoDBTTLCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by AWS DynamoDB with optional TTL support.

    Args:
        table_name: DynamoDB table name.
        dynamodb_client: A boto3 DynamoDB client (or compatible fake).
        ttl_seconds: Optional TTL in seconds. If set, items expire automatically.
        key_prefix: Optional prefix for partition keys.
    """

    def __init__(
        self,
        table_name: str,
        dynamodb_client,
        ttl_seconds: Optional[int] = None,
        key_prefix: str = "checkpoint",
    ):
        self.table_name = table_name
        self._client = dynamodb_client
        self.ttl_seconds = ttl_seconds
        self.key_prefix = key_prefix

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self.key_prefix}:{pipeline_id}:{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        item = {
            "checkpoint_key": {"S": self._key(checkpoint.pipeline_id, checkpoint.stream_id)},
            "pipeline_id": {"S": checkpoint.pipeline_id},
            "stream_id": {"S": checkpoint.stream_id},
            "data": {"S": json.dumps(checkpoint.to_dict())},
        }
        if self.ttl_seconds is not None:
            item["ttl"] = {"N": str(int(time.time()) + self.ttl_seconds)}
        self._client.put_item(TableName=self.table_name, Item=item)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        response = self._client.get_item(
            TableName=self.table_name,
            Key={"checkpoint_key": {"S": self._key(pipeline_id, stream_id)}},
        )
        item = response.get("Item")
        if not item:
            return None
        return Checkpoint.from_dict(json.loads(item["data"]["S"]))

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        self._client.delete_item(
            TableName=self.table_name,
            Key={"checkpoint_key": {"S": self._key(pipeline_id, stream_id)}},
        )

    def list_checkpoints(self, pipeline_id: str):
        prefix = f"{self.key_prefix}:{pipeline_id}:"
        response = self._client.scan(
            TableName=self.table_name,
            FilterExpression="begins_with(checkpoint_key, :pfx)",
            ExpressionAttributeValues={":pfx": {"S": prefix}},
        )
        return [
            Checkpoint.from_dict(json.loads(item["data"]["S"]))
            for item in response.get("Items", [])
        ]
