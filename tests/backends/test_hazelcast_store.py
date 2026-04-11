"""Tests for HazelcastCheckpointStore."""

import json
import pytest

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.hazelcast_store import HazelcastCheckpointStore


class FakeMap:
    def __init__(self):
        self._data = {}

    def set(self, key, value):
        self._data[key] = value

    def get(self, key):
        return self._data.get(key)

    def delete(self, key):
        self._data.pop(key, None)

    def key_set(self):
        return list(self._data.keys())

    def blocking(self):
        return self


class FakeHazelcastClient:
    def __init__(self):
        self._maps = {}

    def get_map(self, name):
        if name not in self._maps:
            self._maps[name] = FakeMap()
        return self._maps[name]


@pytest.fixture
def store():
    client = FakeHazelcastClient()
    return HazelcastCheckpointStore(client, map_name="checkpoints", prefix="ckpt")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42, metadata={"key": "val"})


class TestHazelcastCheckpointStore:
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
        store.delete("pipe1", "missing_stream")

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=99, metadata={})
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 99

    def test_list_checkpoints(self, store):
        cp1 = Checkpoint(pipeline_id="pipe1", stream_id="s1", offset=1, metadata={})
        cp2 = Checkpoint(pipeline_id="pipe1", stream_id="s2", offset=2, metadata={})
        cp3 = Checkpoint(pipeline_id="pipe2", stream_id="s1", offset=3, metadata={})
        store.save(cp1)
        store.save(cp2)
        store.save(cp3)
        results = store.list_checkpoints("pipe1")
        assert len(results) == 2
        offsets = {r.offset for r in results}
        assert offsets == {1, 2}

    def test_list_checkpoints_empty(self, store):
        results = store.list_checkpoints("no_such_pipeline")
        assert results == []

    def test_key_format(self, store, checkpoint):
        store.save(checkpoint)
        raw_map = store._map
        assert "ckpt:pipe1:stream1" in raw_map._data
