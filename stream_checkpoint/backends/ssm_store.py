"""AWS SSM Parameter Store backend for stream-checkpoint."""

import json
from datetime import datetime, timezone

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class SSMCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by AWS SSM Parameter Store.

    Parameters
    ----------
    client:
        A boto3 SSM client (``boto3.client('ssm')``).
    prefix:
        Key prefix applied to every parameter name. Defaults to
        ``"/stream-checkpoint"``.
    tier:
        SSM parameter tier – ``"Standard"`` (default) or ``"Advanced"``.
        Advanced tier supports values up to 8 KB and is required for large
        checkpoint payloads.
    """

    def __init__(self, client, prefix: str = "/stream-checkpoint", tier: str = "Standard"):
        self._client = client
        self._prefix = prefix.rstrip("/")
        self._tier = tier

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _key(self, pipeline_id: str, checkpoint_id: str) -> str:
        return f"{self._prefix}/{pipeline_id}/{checkpoint_id}"

    # ------------------------------------------------------------------
    # BaseCheckpointStore interface
    # ------------------------------------------------------------------

    def save(self, checkpoint: Checkpoint) -> None:
        name = self._key(checkpoint.pipeline_id, checkpoint.checkpoint_id)
        value = json.dumps(checkpoint.to_dict())
        self._client.put_parameter(
            Name=name,
            Value=value,
            Type="String",
            Overwrite=True,
            Tier=self._tier,
        )

    def load(self, pipeline_id: str, checkpoint_id: str):
        name = self._key(pipeline_id, checkpoint_id)
        try:
            response = self._client.get_parameter(Name=name)
            data = json.loads(response["Parameter"]["Value"])
            return Checkpoint.from_dict(data)
        except self._client.exceptions.ParameterNotFound:
            return None

    def delete(self, pipeline_id: str, checkpoint_id: str) -> None:
        name = self._key(pipeline_id, checkpoint_id)
        try:
            self._client.delete_parameter(Name=name)
        except self._client.exceptions.ParameterNotFound:
            pass

    def list_checkpoints(self, pipeline_id: str):
        path = f"{self._prefix}/{pipeline_id}/"
        paginator = self._client.get_paginator("get_parameters_by_path")
        checkpoints = []
        for page in paginator.paginate(Path=path):
            for param in page.get("Parameters", []):
                data = json.loads(param["Value"])
                checkpoints.append(Checkpoint.from_dict(data))
        return checkpoints
