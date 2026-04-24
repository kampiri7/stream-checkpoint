import json
import time
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class CloudWatchCheckpointStore(BaseCheckpointStore):
    """Checkpoint store backed by AWS CloudWatch Logs.

    Each checkpoint is stored as a log event in a dedicated log stream
    within a CloudWatch log group.  The most recent event for a given
    stream/partition key is treated as the current checkpoint.
    """

    def __init__(self, client, log_group: str, prefix: str = "checkpoint") -> None:
        self._client = client
        self._log_group = log_group
        self._prefix = prefix
        self._sequence_tokens: dict = {}
        self._ensure_log_group()

    def _ensure_log_group(self) -> None:
        try:
            self._client.create_log_group(logGroupName=self._log_group)
        except Exception:
            pass  # group already exists

    def _log_stream(self, stream_id: str, partition_key: str) -> str:
        return f"{self._prefix}/{stream_id}/{partition_key}"

    def _ensure_log_stream(self, log_stream: str) -> None:
        try:
            self._client.create_log_stream(
                logGroupName=self._log_group, logStreamName=log_stream
            )
        except Exception:
            pass  # stream already exists

    def save(self, checkpoint: Checkpoint) -> None:
        log_stream = self._log_stream(checkpoint.stream_id, checkpoint.partition_key)
        self._ensure_log_stream(log_stream)
        event = {
            "timestamp": int(time.time() * 1000),
            "message": json.dumps(checkpoint.to_dict()),
        }
        kwargs = {
            "logGroupName": self._log_group,
            "logStreamName": log_stream,
            "logEvents": [event],
        }
        token = self._sequence_tokens.get(log_stream)
        if token:
            kwargs["sequenceToken"] = token
        response = self._client.put_log_events(**kwargs)
        self._sequence_tokens[log_stream] = response.get("nextSequenceToken")

    def load(self, stream_id: str, partition_key: str) -> Optional[Checkpoint]:
        log_stream = self._log_stream(stream_id, partition_key)
        try:
            response = self._client.get_log_events(
                logGroupName=self._log_group,
                logStreamName=log_stream,
                limit=1,
                startFromHead=False,
            )
        except Exception:
            return None
        events = response.get("events", [])
        if not events:
            return None
        return Checkpoint.from_dict(json.loads(events[-1]["message"]))

    def delete(self, stream_id: str, partition_key: str) -> None:
        log_stream = self._log_stream(stream_id, partition_key)
        try:
            self._client.delete_log_stream(
                logGroupName=self._log_group, logStreamName=log_stream
            )
            self._sequence_tokens.pop(log_stream, None)
        except Exception:
            pass

    def list_checkpoints(self, stream_id: str):
        prefix = f"{self._prefix}/{stream_id}/"
        response = self._client.describe_log_streams(
            logGroupName=self._log_group, logStreamNamePrefix=prefix
        )
        results = []
        for ls in response.get("logStreams", []):
            name = ls["logStreamName"]
            partition_key = name[len(prefix):]
            cp = self.load(stream_id, partition_key)
            if cp is not None:
                results.append(cp)
        return results
