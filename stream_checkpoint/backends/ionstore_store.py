"""Amazon Ion (ionstore) checkpoint backend using AWS S3-compatible Ion serialization."""
import json
from ..base import Checkpoint, BaseCheckpointStore


class IonStoreCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Amazon S3 using JSON (Ion-compatible) serialization.

    Parameters
    ----------
    client:
        A boto3 S3 client (or compatible fake).
    bucket: str
        S3 bucket name.
    prefix: str
        Key prefix for all checkpoint objects.
    """

    def __init__(self, client, bucket: str, prefix: str = "checkpoints/"):
        self._client = client
        self._bucket = bucket
        self._prefix = prefix

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}{pipeline_id}/{stream_id}.ion.json"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        body = json.dumps(checkpoint.to_dict()).encode("utf-8")
        self._client.put_object(Bucket=self._bucket, Key=key, Body=body)

    def load(self, pipeline_id: str, stream_id: str):
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
        prefix = f"{self._prefix}{pipeline_id}/"
        response = self._client.list_objects_v2(Bucket=self._bucket, Prefix=prefix)
        results = []
        for obj in response.get("Contents", []):
            resp = self._client.get_object(Bucket=self._bucket, Key=obj["Key"])
            data = json.loads(resp["Body"].read().decode("utf-8"))
            results.append(Checkpoint.from_dict(data))
        return results
