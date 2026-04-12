"""Tests for UpstashCheckpointStore."""

import json
import pytest

from stream_checkpoint.backends.upstash_store import UpstashCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeUpstashClient:
    """Minimal in-memory fake for an Upstash Redis client."""

    def __init__(self):
        self._store: dict = {}

    def set(self, key, value):
        self._store[key] = value

    def setex(self, key, ttl, value):
        # TTL is ignored in the fake; just store the value.
        self._store[key] = value

    def get(self, key):
        return self._store.get(key)

    def delete(self, key):
        self._store.pop(key, None)

    def keys(self, pattern: str):
        prefix = pattern.rstrip("*")
        return [k for k in self._store if k.startswith(prefix)]


@pytest.fixture
def client():
    return FakeUpstashClient()


@pytest.fixture
def store(client):
    return UpstashCheckpointStore(client, prefix="ck")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42)


class TestUpstashCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load("pipe1", "stream1")
        assert loaded is not None
        assert loaded.pipeline_id == "pipe1"
        assert loaded.stream_id == "stream1"
        assert loaded.offset == 42

    def test_load_returns_none_for_missing(self, store):
        assert store.load("nope", "nope") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_delete_nonexistent_is_safe(self, store):
        store.delete("ghost", "ghost")  # should not raise

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=99)
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 99

    def test_list_checkpoints(self, store):
        store.save(Checkpoint(pipeline_id="pipe1", stream_id="s1", offset=1))
        store.save(Checkpoint(pipeline_id="pipe1", stream_id="s2", offset=2))
        store.save(Checkpoint(pipeline_id="other", stream_id="s1", offset=3))
        results = store.list_checkpoints("pipe1")
        assert len(results) == 2
        stream_ids = {r.stream_id for r in results}
        assert stream_ids == {"s1", "s2"}

    def test_ttl_uses_setex(self, client):
        ttl_store = UpstashCheckpointStore(client, prefix="ck", ttl=300)
        cp = Checkpoint(pipeline_id="p", stream_id="s", offset=0)
        ttl_store.save(cp)
        # Verify key was written (setex path)
        key = "ck:p:s"
        assert client.get(key) is not None

    def test_key_format(self, store, checkpoint):
        store.save(checkpoint)
        expected_key = "ck:pipe1:stream1"
        assert store._client.get(expected_key) is not None
