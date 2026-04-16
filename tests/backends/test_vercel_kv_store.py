import json
import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.vercel_kv_store import VercelKVCheckpointStore


class FakeVercelKVClient:
    def __init__(self):
        self._store = {}

    def set(self, key, value):
        self._store[key] = value

    def get(self, key):
        return self._store.get(key)

    def delete(self, key):
        self._store.pop(key, None)

    def keys(self, pattern):
        prefix = pattern.rstrip("*")
        return [k for k in self._store if k.startswith(prefix)]


@pytest.fixture
def client():
    return FakeVercelKVClient()


@pytest.fixture
def store(client):
    return VercelKVCheckpointStore(client, prefix="ckpt")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42, metadata={"key": "val"})


class TestVercelKVCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        result = store.load("pipe1", "stream1")
        assert result is not None
        assert result.offset == 42
        assert result.pipeline_id == "pipe1"

    def test_load_returns_none_for_missing(self, store):
        assert store.load("pipe1", "missing") is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete("pipe1", "stream1")
        assert store.load("pipe1", "stream1") is None

    def test_key_format(self, store):
        assert store._key("p", "s") == "ckpt:p:s"

    def test_list_checkpoints(self, store):
        cp1 = Checkpoint(pipeline_id="pipe1", stream_id="s1", offset=1)
        cp2 = Checkpoint(pipeline_id="pipe1", stream_id="s2", offset=2)
        store.save(cp1)
        store.save(cp2)
        results = store.list_checkpoints("pipe1")
        assert len(results) == 2
        offsets = {r.offset for r in results}
        assert offsets == {1, 2}

    def test_list_checkpoints_empty(self, store):
        assert store.list_checkpoints("no_pipeline") == []

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=99)
        store.save(updated)
        result = store.load("pipe1", "stream1")
        assert result.offset == 99
