"""Azure Blob Storage backend for stream-checkpoint."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class AzureCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by Azure Blob Storage."""

    def __init__(
        self,
        container_client,
        prefix: str = "checkpoints",
    ):
        """
        Initialize the Azure Blob Storage checkpoint store.

        :param container_client: An Azure ``ContainerClient`` instance.
        :param prefix: Blob name prefix for checkpoint keys.
        """
        self._client = container_client
        self._prefix = prefix.rstrip("/")

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        """Build the blob name for the given pipeline/stream pair."""
        return f"{self._prefix}/{pipeline_id}/{stream_id}.json"

    def save(self, checkpoint: Checkpoint) -> None:
        """Upload a checkpoint as a JSON blob."""
        blob_name = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        data = json.dumps(checkpoint.to_dict()).encode("utf-8")
        blob_client = self._client.get_blob_client(blob_name)
        blob_client.upload_blob(data, overwrite=True)

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        """Download and deserialize a checkpoint blob."""
        blob_name = self._key(pipeline_id, stream_id)
        blob_client = self._client.get_blob_client(blob_name)
        try:
            data = blob_client.download_blob().readall()
            return Checkpoint.from_dict(json.loads(data))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        """Delete a checkpoint blob if it exists."""
        blob_name = self._key(pipeline_id, stream_id)
        blob_client = self._client.get_blob_client(blob_name)
        try:
            blob_client.delete_blob()
        except Exception as exc:
            if not _is_not_found(exc):
                raise

    def list_checkpoints(self, pipeline_id: str):
        """List all checkpoints for a given pipeline."""
        prefix = f"{self._prefix}/{pipeline_id}/"
        checkpoints = []
        for blob in self._client.list_blobs(name_starts_with=prefix):
            blob_client = self._client.get_blob_client(blob.name)
            data = blob_client.download_blob().readall()
            checkpoints.append(Checkpoint.from_dict(json.loads(data)))
        return checkpoints


def _is_not_found(exc: Exception) -> bool:
    """Return True if the exception indicates a missing blob."""
    name = type(exc).__name__
    return "ResourceNotFound" in name or "BlobNotFound" in name or "404" in str(exc)
