import json
from datetime import datetime, timezone
from typing import List, Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class DynamoDBCheckpointStore(BaseCheckpointStore):
    """
    DynamoDB-backed checkpoint store.

    Requires boto3 to be installed and AWS credentials configured.
    The table must have a partition key named 'pipeline_id' (String)
    and a sort key named 'stream_id' (String).
    """

    def __init__(
        self,
        table_name: str,
        region_name: str = "us-east-1",
        dynamodb_resource=None,
    ):
        if dynamodb_resource is not None:
            self._table = dynamodb_resource.Table(table_name)
        else:
            import boto3
            dynamodb = boto3.resource("dynamodb", region_name=region_name)
            self._table = dynamodb.Table(table_name)

    def _key(self, pipeline_id: str, stream_id: str) -> dict:
        return {"pipeline_id": pipeline_id, "stream_id": stream_id}

    def save(self, checkpoint: Checkpoint) -> None:
        item = {
            "pipeline_id": checkpoint.pipeline_id,
            "stream_id": checkpoint.stream_id,
            "offset": str(checkpoint.offset),
            "metadata": json.dumps(checkpoint.metadata or {}),
            "updated_at": checkpoint.updated_at.isoformat(),
        }
        self._table.put_item(Item=item)

    def load(
        self, pipeline_id: str, stream_id: str
    ) -> Optional[Checkpoint]:
        response = self._table.get_item(
            Key=self._key(pipeline_id, stream_id)
        )
        item = response.get("Item")
        if item is None:
            return None
        return Checkpoint(
            pipeline_id=item["pipeline_id"],
            stream_id=item["stream_id"],
            offset=int(item["offset"]),
            metadata=json.loads(item["metadata"]),
            updated_at=datetime.fromisoformat(item["updated_at"]),
        )

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        self._table.delete_item(Key=self._key(pipeline_id, stream_id))

    def list_checkpoints(self, pipeline_id: str) -> List[Checkpoint]:
        from boto3.dynamodb.conditions import Key as DynamoKey
        response = self._table.query(
            KeyConditionExpression=DynamoKey("pipeline_id").eq(pipeline_id)
        )
        results = []
        for item in response.get("Items", []):
            results.append(
                Checkpoint(
                    pipeline_id=item["pipeline_id"],
                    stream_id=item["stream_id"],
                    offset=int(item["offset"]),
                    metadata=json.loads(item["metadata"]),
                    updated_at=datetime.fromisoformat(item["updated_at"]),
                )
            )
        return results
