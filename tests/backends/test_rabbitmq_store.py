import json
import pytest

from stream_checkpoint.backends.rabbitmq_store import RabbitMQCheckpointStore
from stream_checkpoint.base import Checkpoint


# ---------------------------------------------------------------------------
# Fake pika primitives
# ---------------------------------------------------------------------------

class FakeMethod:
    def __init__(self, delivery_tag=1):
        self.delivery_tag = delivery_tag


class FakeChannel:
    def __init__(self):
        self._queues: dict[str, list] = {}  # queue_name -> [body, ...]
        self._bindings: dict[str, str] = {}  # routing_key -> queue_name
        self._exchanges: set = set()
        self._nacked: list = []

    def exchange_declare(self, exchange, exchange_type, durable):
        self._exchanges.add(exchange)

    def queue_declare(self, queue, durable, arguments):
        if queue not in self._queues:
            self._queues[queue] = []

    def queue_bind(self, queue, exchange, routing_key):
        self._bindings[routing_key] = queue

    def basic_publish(self, exchange, routing_key, body, properties):
        queue = self._bindings.get(routing_key)
        if queue is not None:
            # Honour x-max-length=1 / drop-head behaviour
            if len(self._queues[queue]) >= 1:
                self._queues[queue].pop(0)
            self._queues[queue].append(body)

    def basic_get(self, queue, auto_ack):
        messages = self._queues.get(queue, [])
        if not messages:
            return None, None, None
        body = messages[0]
        return FakeMethod(), object(), body

    def basic_nack(self, delivery_tag, requeue):
        self._nacked.append((delivery_tag, requeue))

    def queue_purge(self, queue):
        if queue in self._queues:
            self._queues[queue].clear()


class FakeConnection:
    def __init__(self):
        self._channel = FakeChannel()

    def channel(self):
        return self._channel


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store():
    conn = FakeConnection()
    return RabbitMQCheckpointStore(conn, exchange="test_checkpoints")


@pytest.fixture
def checkpoint():
    return Checkpoint(stream_id="river", offset="42", metadata={"lag": 0})


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRabbitMQCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.stream_id)
        assert loaded is not None
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        assert store.load("nonexistent") is None

    def test_save_overwrites_previous(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(stream_id="river", offset="99", metadata={})
        store.save(updated)
        loaded = store.load("river")
        assert loaded.offset == "99"

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.stream_id)
        assert store.load(checkpoint.stream_id) is None

    def test_load_is_non_destructive(self, store, checkpoint):
        store.save(checkpoint)
        first = store.load(checkpoint.stream_id)
        second = store.load(checkpoint.stream_id)
        assert first is not None
        assert second is not None
        assert first.offset == second.offset

    def test_list_checkpoints_returns_empty(self, store, checkpoint):
        store.save(checkpoint)
        assert store.list_checkpoints() == []

    def test_metadata_round_trip(self, store):
        cp = Checkpoint(stream_id="s1", offset="7", metadata={"key": "value", "n": 3})
        store.save(cp)
        loaded = store.load("s1")
        assert loaded.metadata == {"key": "value", "n": 3}
