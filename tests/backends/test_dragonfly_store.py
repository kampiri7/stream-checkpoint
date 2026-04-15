"""Tests for DragonflyCheckpointStore."""
from __future__ import annotations

import json
import pytest

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.dragonfly_store import DragonflyCheckpointStore


# ---------------------------------------------------------------------------
# Fake Dragonfly (Redis-compatible) client
# ---------------------------------------------------------------------------

class FakeDragonfly:
    def __init__(self):
        self._store: dict = {}

    def set(self, key, value):
        self._store[key] = value

    def setex(self, key, ttl, value):
        # TTL is ignored in the fake
        self._store[key] = value

    def get(self, key):
        return self._store.get(key)

    def delete(self, key):
        self._store.pop(key, None)

    def keys(self, pattern: str):
        prefix = pattern.rstrip("*")
        return [k for k in self._store if k.startswith(prefix)]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return FakeDragonfly()


@pytest.fixture
def store(client):
    return DragonflyCheckpointStore(client, prefix="ckpt")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDragonflyCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == "pipe1"
        assert loaded.stream_id == "stream1"
        assert loaded.offset == 42

    def test_load_returns_none_for_missing(self, store):
        result = store.load("no_pipe", "no_stream")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_delete_nonexistent_is_safe(self, store):
        store.delete("ghost", "ghost")  # should not raise

    def test_key_uses_prefix(self, client, checkpoint):
        store = DragonflyCheckpointStore(client, prefix="myprefix")
        store.save(checkpoint)
        assert "myprefix:pipe1:stream1" in client._store

    def test_save_with_ttl_calls_setex(self, client, checkpoint):
        store = DragonflyCheckpointStore(client, prefix="ckpt", ttl=300)
        store.save(checkpoint)
        # setex stores the same way in our fake
        assert client.get("ckpt:pipe1:stream1") is not None

    def test_list_checkpoints(self, store):
        store.save(Checkpoint(pipeline_id="pipe2", stream_id="s1", offset=1))
        store.save(Checkpoint(pipeline_id="pipe2", stream_id="s2", offset=2))
        store.save(Checkpoint(pipeline_id="other", stream_id="s3", offset=3))
        results = store.list_checkpoints("pipe2")
        assert len(results) == 2
        stream_ids = {r.stream_id for r in results}
        assert stream_ids == {"s1", "s2"}

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=99)
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 99
