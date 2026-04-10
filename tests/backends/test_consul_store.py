import json
import pytest

from stream_checkpoint.backends.consul_store import ConsulCheckpointStore
from stream_checkpoint.base import Checkpoint


class FakeKV:
    def __init__(self):
        self._store = {}

    def put(self, key, value):
        self._store[key] = value.encode() if isinstance(value, str) else value

    def get(self, key, recurse=False):
        if recurse:
            prefix = key
            matches = [
                {"Value": v}
                for k, v in self._store.items()
                if k.startswith(prefix)
            ]
            return None, matches if matches else None
        value = self._store.get(key)
        if value is None:
            return None, None
        return None, {"Value": value}

    def delete(self, key):
        self._store.pop(key, None)


class FakeConsulClient:
    def __init__(self):
        self.kv = FakeKV()


@pytest.fixture
def client():
    return FakeConsulClient()


@pytest.fixture
def store(client):
    return ConsulCheckpointStore(client, prefix="test_cp")


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=42,
        metadata={"topic": "events"},
    )


class TestConsulCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        result = store.load("no_pipe", "no_stream")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.pipeline_id, checkpoint.stream_id)
        assert store.load(checkpoint.pipeline_id, checkpoint.stream_id) is None

    def test_key_format(self, store):
        key = store._key("pipeA", "streamB")
        assert key == "test_cp/pipeA/streamB"

    def test_list_checkpoints(self, store):
        cp1 = Checkpoint(pipeline_id="pipe2", stream_id="s1", offset=1)
        cp2 = Checkpoint(pipeline_id="pipe2", stream_id="s2", offset=2)
        store.save(cp1)
        store.save(cp2)
        results = store.list_checkpoints("pipe2")
        assert len(results) == 2
        offsets = {r.offset for r in results}
        assert offsets == {1, 2}

    def test_list_checkpoints_empty(self, store):
        results = store.list_checkpoints("nonexistent_pipe")
        assert results == []

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id=checkpoint.pipeline_id,
            stream_id=checkpoint.stream_id,
            offset=99,
        )
        store.save(updated)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded.offset == 99
