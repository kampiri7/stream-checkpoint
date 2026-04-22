"""Amazon DynamoDB Accelerator (DAX) checkpoint store backend."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class DAXCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Amazon DAX (DynamoDB Accelerator).

    DAX is a fully managed, highly available, in-memory cache for DynamoDB.
    This store uses the DAX client which is API-compatible with boto3 DynamoDB.

    Args:
        client: A DAX client (e.g. from ``amazondax`` or a boto3 DynamoDB client).
        table_name: DynamoDB table name to use for checkpoints.
        key_prefix: Optional prefix applied to all checkpoint keys.
    """

    def __init__(self, client, table_name: str = "stream_checkpoints", key_prefix: str = ""):
        self._client = client
        self._table_name = table_name
        self._prefix = key_prefix

    def _key(self, pipeline: str, stream: str) -> str:
        raw = f"{pipeline}:{stream}"
        return f"{self._prefix}{raw}" if self._prefix else raw

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        self._client.put_item(
            TableName=self._table_name,
            Item={
                "checkpoint_key": {"S": key},
                "data": {"S": json.dumps(checkpoint.to_dict())},
            },
        )

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, stream_id)
        response = self._client.get_item(
            TableName=self._table_name,
            Key={"checkpoint_key": {"S": key}},
        )
        item = response.get("Item")
        if item is None:
            return None
        data = json.loads(item["data"]["S"])
        return Checkpoint.from_dict(data)

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._key(pipeline_id, stream_id)
        self._client.delete_item(
            TableName=self._table_name,
            Key={"checkpoint_key": {"S": key}},
        )

    def list_checkpoints(self, pipeline_id: str):
        prefix = self._key(pipeline_id, "")
        response = self._client.scan(
            TableName=self._table_name,
            FilterExpression="begins_with(checkpoint_key, :pfx)",
            ExpressionAttributeValues={
                ":pfx": {"S": prefix},
            },
        )
        results = []
        for item in response.get("Items", []):
            data = json.loads(item["data"]["S"])
            results.append(Checkpoint.from_dict(data))
        return results
