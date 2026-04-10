"""Google Cloud Storage backend for stream-checkpoint."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class GCSCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Google Cloud Storage."""

    def __init__(self, bucket_name: str, prefix: str = "checkpoints/", client=None):
        """
        Initialize the GCS checkpoint store.

        Args:
            bucket_name: Name of the GCS bucket.
            prefix: Key prefix for all checkpoint objects.
            client: Optional GCS client (injected for testing).
        """
        self.bucket_name = bucket_name
        self.prefix = prefix

        if client is None:
            from google.cloud import storage  # type: ignore
            client = storage.Client()

        self._client = client
        self._bucket = self._client.bucket(bucket_name)

    def _key(self, pipeline_id: str, checkpoint_id: str) -> str:
        return f"{self.prefix}{pipeline_id}/{checkpoint_id}.json"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.checkpoint_id)
        blob = self._bucket.blob(key)
        blob.upload_from_string(
            json.dumps(checkpoint.to_dict()),
            content_type="application/json",
        )

    def load(self, pipeline_id: str, checkpoint_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, checkpoint_id)
        blob = self._bucket.blob(key)
        try:
            data = blob.download_as_text()
        except Exception:
            return None
        return Checkpoint.from_dict(json.loads(data))

    def delete(self, pipeline_id: str, checkpoint_id: str) -> None:
        key = self._key(pipeline_id, checkpoint_id)
        blob = self._bucket.blob(key)
        try:
            blob.delete()
        except Exception:
            pass

    def list_checkpoints(self, pipeline_id: str):
        prefix = f"{self.prefix}{pipeline_id}/"
        blobs = self._client.list_blobs(self.bucket_name, prefix=prefix)
        checkpoints = []
        for blob in blobs:
            try:
                data = blob.download_as_text()
                checkpoints.append(Checkpoint.from_dict(json.loads(data)))
            except Exception:
                continue
        return checkpoints
