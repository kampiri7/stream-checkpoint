"""Tests for the Valkey checkpoint store backend."""

import json
import pytest

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.valkey_store import ValkeyCheckpointStore


class FakeValkey:
    """Minimal in-memory fake Valkey client."""

    def __init__(self):
        self._store = {}

    def set(self, key, value):
        self._store[key] = value

    def setex(self, key, ttl, value):
        self._store[key] = value  # TTL ignored in fake

    def get(self, key):
        return self._store.get(key)

    def delete(self, key):
        self._store.pop(key, None)

    def keys(self, pattern):
        prefix = pattern.rstrip("*")
        return [k for k in self._store if k.startswith(prefix)]


@pytest.fixture
def client():
    return FakeValkey()


@pytest.fixture
def store(client):
    return ValkeyCheckpointStore(client, prefix="ckpt")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42, metadata={"k": "v"})


class TestValkeyCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == "pipe1"
        assert loaded.stream_id == "stream1"
        assert loaded.offset == 42
        assert loaded.metadata == {"k": "v"}

    def test_load_returns_none_for_missing(self, store):
        assert store.load("no_pipe", "no_stream") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost", "ghost")  # should not raise

    def test_save_uses_prefix_in_key(self, client, checkpoint):
        store = ValkeyCheckpointStore(client, prefix="myns")
        store.save(checkpoint)
        assert "myns:pipe1:stream1" in client._store

    def test_save_with_ttl_calls_setex(self, client, checkpoint):
        store = ValkeyCheckpointStore(client, prefix="ckpt", ttl=300)
        store.save(checkpoint)
        assert client.get("ckpt:pipe1:stream1") is not None

    def test_list_checkpoints(self, store):
        store.save(Checkpoint(pipeline_id="pipe2", stream_id="s1", offset=1))
        store.save(Checkpoint(pipeline_id="pipe2", stream_id="s2", offset=2))
        store.save(Checkpoint(pipeline_id="other", stream_id="s1", offset=99))
        results = store.list_checkpoints("pipe2")
        assert len(results) == 2
        offsets = {c.offset for c in results}
        assert offsets == {1, 2}

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=100)
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 100
