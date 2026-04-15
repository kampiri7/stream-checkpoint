import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class MinIOCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by MinIO (S3-compatible object storage)."""

    def __init__(
        self,
        client,
        bucket: str,
        prefix: str = "checkpoints",
    ):
        """
        Args:
            client: A MinIO client instance (``minio.Minio``).
            bucket: Name of the bucket to store checkpoints in.
            prefix: Key prefix used for all checkpoint objects.
        """
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}/{pipeline_id}/{stream_id}.json"

    def save(self, checkpoint: Checkpoint) -> None:
        import io

        data = json.dumps(checkpoint.to_dict()).encode("utf-8")
        self._client.put_object(
            self._bucket,
            self._key(checkpoint.pipeline_id, checkpoint.stream_id),
            io.BytesIO(data),
            length=len(data),
            content_type="application/json",
        )

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        from minio.error import S3Error

        try:
            response = self._client.get_object(
                self._bucket,
                self._key(pipeline_id, stream_id),
            )
            data = json.loads(response.read().decode("utf-8"))
            return Checkpoint.from_dict(data)
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                return None
            raise

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        self._client.remove_object(
            self._bucket,
            self._key(pipeline_id, stream_id),
        )

    def list_checkpoints(self, pipeline_id: str):
        prefix = f"{self._prefix}/{pipeline_id}/"
        objects = self._client.list_objects(self._bucket, prefix=prefix)
        checkpoints = []
        for obj in objects:
            response = self._client.get_object(self._bucket, obj.object_name)
            data = json.loads(response.read().decode("utf-8"))
            checkpoints.append(Checkpoint.from_dict(data))
        return checkpoints
