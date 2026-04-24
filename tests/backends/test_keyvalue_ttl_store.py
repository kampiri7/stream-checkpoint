"""Tests for KeyValueTTLCheckpointStore."""

import json
import time
import pytest

from stream_checkpoint.backends.keyvalue_ttl_store import KeyValueTTLCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeKVClient:
    def __init__(self):
        self._store = {}
        self.last_ttl = {}

    def set(self, key, value, ttl_seconds=None):
        self._store[key] = value
        self.last_ttl[key] = ttl_seconds

    def get(self, key):
        return self._store.get(key)

    def delete(self, key):
        self._store.pop(key, None)
        self.last_ttl.pop(key, None)

    def keys(self, prefix):
        return [k for k in self._store if k.startswith(prefix)]


@pytest.fixture
def client():
    return FakeKVClient()


@pytest.fixture
def store(client):
    return KeyValueTTLCheckpointStore(client, prefix="ck:", ttl=300)


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42)


class TestKeyValueTTLCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        result = store.load("pipe1", "stream1")
        assert result is not None
        assert result.offset == 42

    def test_save_sets_ttl(self, client, store, checkpoint):
        store.save(checkpoint)
        key = "ck:pipe1:stream1"
        assert client.last_ttl[key] == 300

    def test_save_without_ttl_passes_none(self, client, checkpoint):
        s = KeyValueTTLCheckpointStore(client, prefix="ck:", ttl=None)
        s.save(checkpoint)
        assert client.last_ttl["ck:pipe1:stream1"] is None

    def test_load_returns_none_for_missing(self, store):
        assert store.load("no_pipe", "no_stream") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_list_checkpoints(self, store):
        store.save(Checkpoint(pipeline_id="pipe1", stream_id="s1", offset=1))
        store.save(Checkpoint(pipeline_id="pipe1", stream_id="s2", offset=2))
        store.save(Checkpoint(pipeline_id="pipe2", stream_id="s1", offset=99))
        results = store.list_checkpoints("pipe1")
        assert len(results) == 2
        offsets = {r.offset for r in results}
        assert offsets == {1, 2}

    def test_list_checkpoints_empty(self, store):
        assert store.list_checkpoints("nonexistent") == []

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=100)
        store.save(updated)
        result = store.load("pipe1", "stream1")
        assert result.offset == 100
