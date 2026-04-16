import json
from datetime import datetime, timezone
from stream_checkpoint.base import Checkpoint, BaseCheckpointStore


class SQSCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by AWS SQS (via a companion S3 bucket for state).

    Since SQS is a queue and not a key-value store, this backend uses an
    S3 bucket as the durable state store and optionally publishes a
    notification message to an SQS queue on every save.
    """

    def __init__(self, s3_client, sqs_client, bucket: str, queue_url: str, prefix: str = "checkpoints/"):
        self._s3 = s3_client
        self._sqs = sqs_client
        self._bucket = bucket
        self._queue_url = queue_url
        self._prefix = prefix

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}{pipeline_id}/{stream_id}.json"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        body = json.dumps(checkpoint.to_dict())
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=body)
        self._sqs.send_message(
            QueueUrl=self._queue_url,
            MessageBody=json.dumps({
                "event": "checkpoint_saved",
                "pipeline_id": checkpoint.pipeline_id,
                "stream_id": checkpoint.stream_id,
                "offset": checkpoint.offset,
            }),
        )

    def load(self, pipeline_id: str, stream_id: str):
        key = self._key(pipeline_id, stream_id)
        try:
            response = self._s3.get_object(Bucket=self._bucket, Key=key)
            data = json.loads(response["Body"].read())
            return Checkpoint.from_dict(data)
        except self._s3.exceptions.NoSuchKey:
            return None

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._key(pipeline_id, stream_id)
        self._s3.delete_object(Bucket=self._bucket, Key=key)

    def list_checkpoints(self, pipeline_id: str):
        prefix = f"{self._prefix}{pipeline_id}/"
        response = self._s3.list_objects_v2(Bucket=self._bucket, Prefix=prefix)
        checkpoints = []
        for obj in response.get("Contents", []):
            resp = self._s3.get_object(Bucket=self._bucket, Key=obj["Key"])
            data = json.loads(resp["Body"].read())
            checkpoints.append(Checkpoint.from_dict(data))
        return checkpoints
