"""Unit tests for KafkaCheckpointStore using lightweight fakes."""

import json
from typing import Dict, List, Optional

import pytest

from stream_checkpoint.backends.kafka_store import KafkaCheckpointStore
from stream_checkpoint.base import Checkpoint


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeProducer:
    """Minimal Kafka producer fake that stores messages in memory."""

    def __init__(self):
        self.messages: List[dict] = []
        self.flush_called = 0

    def produce(self, topic: str, key: bytes, value: Optional[bytes]) -> None:
        self.messages.append({"topic": topic, "key": key, "value": value})

    def flush(self) -> None:
        self.flush_called += 1


class FakeConsumer:
    """Minimal Kafka consumer fake backed by the same in-memory dict."""

    def __init__(self, producer: FakeProducer):
        self._producer = producer

    def _latest_per_key(self, topic: str) -> Dict[bytes, Optional[bytes]]:
        state: Dict[bytes, Optional[bytes]] = {}
        for msg in self._producer.messages:
            if msg["topic"] == topic:
                state[msg["key"]] = msg["value"]
        return state

    def get_latest(self, topic: str, key: str) -> Optional[bytes]:
        state = self._latest_per_key(topic)
        raw = state.get(key.encode())
        return raw  # None means tombstoned / not found

    def get_all(self, topic: str, pipeline: str) -> List[Optional[bytes]]:
        state = self._latest_per_key(topic)
        results = []
        prefix = f"{pipeline}::".encode()
        for k, v in state.items():
            if k.startswith(prefix) and v is not None:
                results.append(v)
        return results


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def producer():
    return FakeProducer()


@pytest.fixture()
def store(producer):
    consumer = FakeConsumer(producer)
    return KafkaCheckpointStore(producer, consumer, topic="test_checkpoints")


@pytest.fixture()
def checkpoint():
    return Checkpoint(pipeline="etl", stream_id="s1", offset=42, metadata={"k": "v"})


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestKafkaCheckpointStore:
    def test_save_produces_message(self, store, producer, checkpoint):
        store.save(checkpoint)
        assert len(producer.messages) == 1
        msg = producer.messages[0]
        assert msg["topic"] == "test_checkpoints"
        assert msg["key"] == b"etl::s1"

    def test_save_calls_flush(self, store, producer, checkpoint):
        store.save(checkpoint)
        assert producer.flush_called == 1

    def test_save_and_load_roundtrip(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("etl", "s1")
        assert loaded is not None
        assert loaded.pipeline == checkpoint.pipeline
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.offset == checkpoint.offset
        assert loaded.metadata == checkpoint.metadata

    def test_load_returns_none_for_missing(self, store):
        assert store.load("etl", "nonexistent") is None

    def test_delete_produces_tombstone(self, store, producer, checkpoint):
        store.save(checkpoint)
        store.delete("etl", "s1")
        assert producer.messages[-1]["value"] is None
        assert store.load("etl", "s1") is None

    def test_list_checkpoints(self, store, checkpoint):
        store.save(checkpoint)
        cp2 = Checkpoint(pipeline="etl", stream_id="s2", offset=99)
        store.save(cp2)
        results = store.list_checkpoints("etl")
        stream_ids = {cp.stream_id for cp in results}
        assert stream_ids == {"s1", "s2"}

    def test_list_checkpoints_excludes_deleted(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("etl", "s1")
        results = store.list_checkpoints("etl")
        assert results == []

    def test_list_checkpoints_excludes_other_pipelines(self, store, checkpoint):
        store.save(checkpoint)
        other = Checkpoint(pipeline="other", stream_id="s9", offset=1)
        store.save(other)
        results = store.list_checkpoints("etl")
        assert all(cp.pipeline == "etl" for cp in results)
