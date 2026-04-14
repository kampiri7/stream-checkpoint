import json
import pytest

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.tarantool_store import TarantoolCheckpointStore


class FakeSpace:
    """Minimal fake of a Tarantool space."""

    def __init__(self):
        self._data = {}  # key -> json_string

    def replace(self, tuple_):
        key, value = tuple_
        self._data[key] = value

    def select(self, key=None):
        if key is None:
            return [(k, v) for k, v in self._data.items()]
        if key in self._data:
            return [(key, self._data[key])]
        return []

    def delete(self, key):
        self._data.pop(key, None)


class FakeTarantoolClient:
    """Minimal fake of a tarantool.Connection."""

    def __init__(self):
        self._spaces = {}

    def space(self, name: str) -> FakeSpace:
        if name not in self._spaces:
            self._spaces[name] = FakeSpace()
        return self._spaces[name]


@pytest.fixture
def client():
    return FakeTarantoolClient()


@pytest.fixture
def store(client):
    return TarantoolCheckpointStore(client, space="checkpoints", prefix="ckpt")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42, metadata={"k": "v"})


class TestTarantoolCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.offset == checkpoint.offset
        assert loaded.metadata == checkpoint.metadata

    def test_load_returns_none_for_missing(self, store):
        result = store.load("no_pipe", "no_stream")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.pipeline_id, checkpoint.stream_id)
        assert store.load(checkpoint.pipeline_id, checkpoint.stream_id) is None

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost", "ghost_stream")

    def test_overwrite_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id=checkpoint.pipeline_id,
            stream_id=checkpoint.stream_id,
            offset=99,
            metadata={},
        )
        store.save(updated)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded.offset == 99

    def test_list_checkpoints(self, store):
        for i in range(3):
            store.save(Checkpoint(pipeline_id="pipe1", stream_id=f"s{i}", offset=i))
        store.save(Checkpoint(pipeline_id="other", stream_id="sx", offset=0))
        result = store.list_checkpoints("pipe1")
        assert len(result) == 3
        stream_ids = {c.stream_id for c in result}
        assert stream_ids == {"s0", "s1", "s2"}

    def test_key_uses_prefix_and_ids(self, store):
        key = store._key("p", "s")
        assert key == "ckpt:p:s"
