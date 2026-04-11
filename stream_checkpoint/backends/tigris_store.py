import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class TigrisCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by Tigris (S3-compatible object storage).

    Parameters
    ----------
    client:
        A Tigris / S3-compatible client object (e.g. boto3 client configured
        for Tigris).  Must expose ``put_object``, ``get_object``, and
        ``delete_object`` methods.
    bucket : str
        Name of the Tigris bucket to store checkpoints in.
    prefix : str, optional
        Key prefix applied to every checkpoint key (default ``"checkpoints/"``).
    """

    def __init__(self, client, bucket: str, prefix: str = "checkpoints/") -> None:
        self._client = client
        self._bucket = bucket
        self._prefix = prefix

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}{pipeline_id}/{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        body = json.dumps(checkpoint.to_dict()).encode("utf-8")
        self._client.put_object(Bucket=self._bucket, Key=key, Body=body)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, stream_id)
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            data = json.loads(response["Body"].read().decode("utf-8"))
            return Checkpoint.from_dict(data)
        except self._client.exceptions.NoSuchKey:
            return None

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        key = self._key(pipeline_id, stream_id)
        self._client.delete_object(Bucket=self._bucket, Key=key)

    def list_checkpoints(self, pipeline_id: str):
        prefix = self._key(pipeline_id, "")
        response = self._client.list_objects_v2(Bucket=self._bucket, Prefix=prefix)
        checkpoints = []
        for obj in response.get("Contents", []):
            resp = self._client.get_object(Bucket=self._bucket, Key=obj["Key"])
            data = json.loads(resp["Body"].read().decode("utf-8"))
            checkpoints.append(Checkpoint.from_dict(data))
        return checkpoints
