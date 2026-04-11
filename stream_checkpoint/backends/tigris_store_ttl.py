import json
import time
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class TigrisTTLCheckpointStore(BaseCheckpointStore):
    """
    Tigris-backed checkpoint store with client-side TTL support.

    Checkpoints are stored as JSON with an embedded ``_expires_at`` field.
    On ``load`` any expired checkpoint is treated as missing and is deleted.

    Parameters
    ----------
    client:
        S3-compatible Tigris client.
    bucket : str
        Bucket name.
    ttl_seconds : int, optional
        Lifetime of each checkpoint in seconds.  ``None`` means no expiry.
    prefix : str, optional
        Key prefix (default ``"checkpoints-ttl/"``).
    """

    def __init__(
        self,
        client,
        bucket: str,
        ttl_seconds: Optional[int] = None,
        prefix: str = "checkpoints-ttl/",
    ) -> None:
        self._client = client
        self._bucket = bucket
        self._ttl = ttl_seconds
        self._prefix = prefix

    def _key(self, pipeline_id: str, stream_id: str) -> str:
        return f"{self._prefix}{pipeline_id}/{stream_id}"

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline_id, checkpoint.stream_id)
        payload = checkpoint.to_dict()
        if self._ttl is not None:
            payload["_expires_at"] = time.time() + self._ttl
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=json.dumps(payload).encode("utf-8"),
        )

    def load(self, pipeline_id: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline_id, stream_id)
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            payload = json.loads(response["Body"].read().decode("utf-8"))
        except self._client.exceptions.NoSuchKey:
            return None

        expires_at = payload.pop("_expires_at", None)
        if expires_at is not None and time.time() > expires_at:
            self.delete(pipeline_id, stream_id)
            return None

        return Checkpoint.from_dict(payload)

    def delete(self, pipeline_id: str, stream_id: str) -> None:
        self._client.delete_object(
            Bucket=self._bucket,
            Key=self._key(pipeline_id, stream_id),
        )
