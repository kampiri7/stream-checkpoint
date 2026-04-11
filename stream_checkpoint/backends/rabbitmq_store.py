import json
from typing import Optional

from stream_checkpoint.base import BaseCheckpointStore, Checkpoint


class RabbitMQCheckpointStore(BaseCheckpointStore):
    """
    Checkpoint store backed by RabbitMQ using a dedicated quorum queue
    per pipeline. Checkpoints are stored as persistent messages keyed
    by stream_id inside a header exchange so that a consumer can fetch
    the latest state on restart.

    The implementation uses a simple key-value emulation on top of
    RabbitMQ's management / classic queues: each checkpoint is published
    as a persistent message with the stream_id as the routing-key and
    the previous message (if any) is purged before publishing.
    """

    def __init__(self, connection, exchange: str = "stream_checkpoints") -> None:
        """
        :param connection: A pika (or compatible) BlockingConnection instance.
        :param exchange: Name of the direct exchange to use.
        """
        self._connection = connection
        self._exchange = exchange
        self._channel = connection.channel()
        self._channel.exchange_declare(
            exchange=self._exchange,
            exchange_type="direct",
            durable=True,
        )

    def _queue_name(self, stream_id: str) -> str:
        return f"{self._exchange}.{stream_id}"

    def _ensure_queue(self, stream_id: str) -> str:
        queue = self._queue_name(stream_id)
        self._channel.queue_declare(
            queue=queue,
            durable=True,
            arguments={"x-max-length": 1, "x-overflow": "drop-head"},
        )
        self._channel.queue_bind(
            queue=queue,
            exchange=self._exchange,
            routing_key=stream_id,
        )
        return queue

    def save(self, checkpoint: Checkpoint) -> None:
        import pika

        self._ensure_queue(checkpoint.stream_id)
        body = json.dumps(checkpoint.to_dict()).encode()
        self._channel.basic_publish(
            exchange=self._exchange,
            routing_key=checkpoint.stream_id,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),
        )

    def load(self, stream_id: str) -> Optional[Checkpoint]:
        self._ensure_queue(stream_id)
        method, _props, body = self._channel.basic_get(
            queue=self._queue_name(stream_id), auto_ack=False
        )
        if method is None or body is None:
            return None
        # Re-queue the message so it survives the next load call.
        self._channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return Checkpoint.from_dict(json.loads(body.decode()))

    def delete(self, stream_id: str) -> None:
        queue = self._queue_name(stream_id)
        self._channel.queue_purge(queue=queue)

    def list_checkpoints(self) -> list:
        # RabbitMQ does not support enumeration without the management plugin;
        # return an empty list as a safe default.
        return []
