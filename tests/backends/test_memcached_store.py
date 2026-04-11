import json
import pytest

from stream_checkpoint.backends.memcached_store import MemcachedCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeMemcachedClient:
    """Minimal in-memory mock for a pymemcache-compatible client."""

    def __init__(self):
        self._store = {}

    def set(self, key, value, expire=0):
        self._store[key] = value

    def get(self, key):
        return self._store.get(key)

    def delete(self, key):
        self._store.pop(key, None)


@pytest.fixture
def client():
    return FakeMemcachedClient()


@pytest.fixture
def store(client):
    return MemcachedCheckpointStore(client, prefix="ck:", ttl=300)


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42)


class TestMemcachedCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == "pipe1"
        assert loaded.stream_id == "stream1"
        assert loaded.offset == 42

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nonexistent", "stream")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost", "stream")

    def test_key_uses_prefix(self, client, checkpoint):
        store = MemcachedCheckpointStore(client, prefix="myapp:", ttl=0)
        store.save(checkpoint)
        expected_key = "myapp:pipe1:stream1"
        assert expected_key in client._store

    def test_saved_value_is_valid_json(self, client, store, checkpoint):
        store.save(checkpoint)
        raw = client._store.get("ck:pipe1:stream1")
        assert raw is not None
        data = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        assert data["pipeline_id"] == "pipe1"
        assert data["offset"] == 42

    def test_overwrite_updates_offset(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=99)
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 99

    def test_list_checkpoints_raises(self, store):
        with pytest.raises(NotImplementedError):
            store.list_checkpoints("pipe1")
