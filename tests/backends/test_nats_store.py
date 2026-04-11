import json
import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.nats_store import NATSCheckpointStore


class FakeEntry:
    def __init__(self, key: str, value: bytes):
        self.key = key
        self.value = value


class FakeKV:
    """Minimal in-memory fake for a NATS JetStream KeyValue bucket."""

    def __init__(self):
        self._store: dict = {}

    def put(self, key: str, value: bytes) -> None:
        self._store[key] = value

    def get(self, key: str) -> FakeEntry:
        if key not in self._store:
            raise KeyError(key)
        return FakeEntry(key, self._store[key])

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def keys(self):
        return list(self._store.keys())


@pytest.fixture
def kv():
    return FakeKV()


@pytest.fixture
def store(kv):
    return NATSCheckpointStore(kv, prefix="ckpt")


@pytest.fixture
def checkpoint():
    return Checkpoint(
        pipeline_id="pipe1",
        stream_id="stream1",
        offset=42,
        metadata={"source": "nats"},
    )


class TestNATSCheckpointStore:
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

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost", "ghost_stream")

    def test_list_checkpoints(self, store):
        for i in range(3):
            cp = Checkpoint(pipeline_id="pipe1", stream_id=f"s{i}", offset=i)
            store.save(cp)
        other = Checkpoint(pipeline_id="pipe2", stream_id="sx", offset=99)
        store.save(other)
        results = store.list_checkpoints("pipe1")
        assert len(results) == 3
        stream_ids = {r.stream_id for r in results}
        assert stream_ids == {"s0", "s1", "s2"}

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id="pipe1", stream_id="stream1", offset=100
        )
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 100

    def test_key_uses_prefix(self, kv, checkpoint):
        store = NATSCheckpointStore(kv, prefix="myapp")
        store.save(checkpoint)
        assert "myapp.pipe1.stream1" in kv._store
