import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class S3CheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by Amazon S3.

    Each checkpoint is stored as a JSON object at:
        s3://<bucket>/<prefix><pipeline_id>/<checkpoint_id>.json
    """

    def __init__(self, bucket: str, prefix: str = "checkpoints/", s3_client=None):
        """
        Parameters
        ----------
        bucket:
            Name of the S3 bucket.
        prefix:
            Key prefix (folder) used for all checkpoint objects.
        s3_client:
            An existing boto3 S3 client.  If *None* a default client is
            created via ``boto3.client('s3')``.
        """
        self.bucket = bucket
        self.prefix = prefix.rstrip("/") + "/"
        if s3_client is None:
            import boto3  # pragma: no cover
            s3_client = boto3.client("s3")  # pragma: no cover
        self._client = s3_client

    def _key(self, pipeline_id: str, checkpoint_id: str) -> str:
        return f"{self.prefix}{pipeline_id}/{checkpoint_id}.json"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.checkpoint_id)
        body = json.dumps(checkpoint.to_dict()).encode("utf-8")
        self._client.put_object(Bucket=self.bucket, Key=key, Body=body)

    def load(self, pipeline_id: str, checkpoint_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, checkpoint_id)
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            return Checkpoint.from_dict(data)
        except self._client.exceptions.NoSuchKey:
            return None

    def delete(self, pipeline_id: str, checkpoint_id: str) -> None:
        key = self._key(pipeline_id, checkpoint_id)
        self._client.delete_object(Bucket=self.bucket, Key=key)

    def list_checkpoints(self, pipeline_id: str):
        prefix = f"{self.prefix}{pipeline_id}/"
        response = self._client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        checkpoints = []
        for obj in response.get("Contents", []):
            resp = self._client.get_object(Bucket=self.bucket, Key=obj["Key"])
            data = json.loads(resp["Body"].read().decode("utf-8"))
            checkpoints.append(Checkpoint.from_dict(data))
        return checkpoints
