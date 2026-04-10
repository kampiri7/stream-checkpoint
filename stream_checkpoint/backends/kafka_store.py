"""Kafka-backed checkpoint store using a compacted topic as persistent storage."""

import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class KafkaCheckpointStore(BaseCheckpointStore):
    """Checkpoint store that persists state to a Kafka compacted topic.

    Each checkpoint is written as a key/value message where the key is the
    pipeline + stream_id composite and the value is the serialised checkpoint
    JSON.  A tombstone (value=None) is produced on delete.

    Parameters
    ----------
    producer:
        A Kafka producer instance (e.g. ``confluent_kafka.Producer`` or any
        object exposing ``produce(topic, key, value)`` / ``flush()``).
    consumer:
        A Kafka consumer instance used for point-in-time reads.  Must expose
        ``assign``, ``poll``, and ``get_watermark_offsets``.
    topic:
        Name of the compacted Kafka topic used for checkpoint storage.
    """

    def __init__(self, producer, consumer, topic: str = "stream_checkpoints") -> None:
        self._producer = producer
        self._consumer = consumer
        self._topic = topic

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _key(self, pipeline: str, stream_id: str) -> str:
        return f"{pipeline}::{stream_id}"

    # ------------------------------------------------------------------
    # BaseCheckpointStore interface
    # ------------------------------------------------------------------

    def save(self, checkpoint: Checkpoint) -> None:
        key = self._key(checkpoint.pipeline, checkpoint.stream_id)
        value = json.dumps(checkpoint.to_dict())
        self._producer.produce(
            topic=self._topic,
            key=key.encode(),
            value=value.encode(),
        )
        self._producer.flush()

    def load(self, pipeline: str, stream_id: str) -> Optional[Checkpoint]:
        key = self._key(pipeline, stream_id)
        raw = self._consumer.get_latest(self._topic, key)
        if raw is None:
            return None
        return Checkpoint.from_dict(json.loads(raw))

    def delete(self, pipeline: str, stream_id: str) -> None:
        key = self._key(pipeline, stream_id)
        # Tombstone message removes the key from the compacted topic.
        self._producer.produce(
            topic=self._topic,
            key=key.encode(),
            value=None,
        )
        self._producer.flush()

    def list_checkpoints(self, pipeline: str):
        raw_messages = self._consumer.get_all(self._topic, pipeline)
        checkpoints = []
        for raw in raw_messages:
            if raw is not None:
                checkpoints.append(Checkpoint.from_dict(json.loads(raw)))
        return checkpoints
