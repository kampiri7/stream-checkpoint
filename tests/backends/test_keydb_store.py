"""Tests for the KeyDB checkpoint store backend."""

import json
import pytest

from stream_checkpoint.backends.keydb_store import KeyDBCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeKeyDB:
    """Minimal in-memory KeyDB/Redis-compatible fake client."""

    def __init__(self):
        self._store = {}

    def set(self, key, value):
        self._store[key] = value

    def setex(self, key, ttl, value):
        # TTL is ignored in the fake — just store the value.
        self._store[key] = value

    def get(self, key):
        return self._store.get(key)

    def delete(self, key):
        self._store.pop(key, None)

    def keys(self, pattern):
        import fnmatch
        return [k for k in self._store if fnmatch.fnmatch(k, pattern)]


@pytest.fixture
def client():
    return FakeKeyDB()


@pytest.fixture
def store(client):
    return KeyDBCheckpointStore(client, prefix="ckpt")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42)


class TestKeyDBCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == "pipe1"
        assert loaded.stream_id == "stream1"
        assert loaded.offset == 42

    def test_load_returns_none_for_missing(self, store):
        assert store.load("no_pipe", "no_stream") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_delete_nonexistent_is_noop(self, store):
        store.delete("ghost", "ghost")  # should not raise

    def test_save_with_ttl(self, client, checkpoint):
        ttl_store = KeyDBCheckpointStore(client, prefix="ckpt", ttl=300)
        ttl_store.save(checkpoint)
        raw = client.get("ckpt:pipe1:stream1")
        assert raw is not None
        data = json.loads(raw)
        assert data["offset"] == 42

    def test_list_checkpoints(self, store):
        store.save(Checkpoint(pipeline_id="pipe2", stream_id="s1", offset=1))
        store.save(Checkpoint(pipeline_id="pipe2", stream_id="s2", offset=2))
        store.save(Checkpoint(pipeline_id="other", stream_id="s1", offset=99))
        results = store.list_checkpoints("pipe2")
        assert len(results) == 2
        offsets = {c.offset for c in results}
        assert offsets == {1, 2}

    def test_key_format(self, store):
        assert store._key("p", "s") == "ckpt:p:s"

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=100)
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 100
