import json
import pytest
from stream_checkpoint.backends.upstash_ttl_store import UpstashTTLCheckpointStore
from stream_checkpoint.base import Checkpoint
from datetime import datetime


class FakeUpstashTTLClient:
    def __init__(self):
        self._store = {}
        self._ttls = {}

    def setex(self, key, ttl, value):
        self._store[key] = value
        self._ttls[key] = ttl

    def get(self, key):
        return self._store.get(key)

    def delete(self, key):
        self._store.pop(key, None)

    def keys(self, pattern):
        prefix = pattern.rstrip("*")
        return [k for k in self._store if k.startswith(prefix)]


@pytest.fixture
def client():
    return FakeUpstashTTLClient()


@pytest.fixture
def store(client):
    return UpstashTTLCheckpointStore(client, prefix="ck", ttl=300)


@pytest.fixture
def checkpoint():
    return Checkpoint(
        stream_id="orders",
        partition="0",
        offset="42",
        metadata={"source": "kafka"},
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
    )


class TestUpstashTTLCheckpointStore:
    def test_save_sets_ttl(self, store, client, checkpoint):
        store.save(checkpoint)
        key = "ck:orders:0"
        assert key in client._store
        assert client._ttls[key] == 300

    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        result = store.load("orders", "0")
        assert result is not None
        assert result.stream_id == "orders"
        assert result.offset == "42"

    def test_load_returns_none_for_missing(self, store):
        assert store.load("missing", "0") is None

    def test_delete_removes_key(self, store, client, checkpoint):
        store.save(checkpoint)
        store.delete("orders", "0")
        assert store.load("orders", "0") is None

    def test_list_partitions(self, store, checkpoint):
        store.save(checkpoint)
        cp2 = Checkpoint(stream_id="orders", partition="1", offset="10", metadata={}, timestamp=datetime(2024, 1, 1))
        store.save(cp2)
        partitions = store.list_partitions("orders")
        assert set(partitions) == {"0", "1"}

    def test_metadata_preserved(self, store, checkpoint):
        store.save(checkpoint)
        result = store.load("orders", "0")
        assert result.metadata == {"source": "kafka"}
