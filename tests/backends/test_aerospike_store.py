import json
import pytest

from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.aerospike_store import AerospikeCheckpointStore


class FakeAerospikeClient:
    """Minimal Aerospike client stub."""

    POLICY_EXISTS_IGNORE = 0

    def __init__(self):
        self._store = {}

    def _str_key(self, key):
        ns, set_name, pk = key
        return f"{ns}/{set_name}/{pk}"

    def put(self, key, data, meta=None, policy=None):
        self._store[self._str_key(key)] = data

    def get(self, key):
        k = self._str_key(key)
        record = self._store.get(k)
        return key, {}, record

    def remove(self, key):
        k = self._str_key(key)
        self._store.pop(k, None)

    def scan(self, namespace, set_name):
        return FakeScan(self._store)


class FakeScan:
    def __init__(self, store):
        self._store = store

    def foreach(self, callback):
        for k, bins in self._store.items():
            fake_key = tuple(k.split("/", 2))
            callback((fake_key, {}, bins))


@pytest.fixture
def client():
    return FakeAerospikeClient()


@pytest.fixture
def store(client):
    return AerospikeCheckpointStore(client, namespace="test_ns", set_name="test_set")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42)


class TestAerospikeCheckpointStore:
    def test_save_and_load(self, store, checkpoint):
        store.save(checkpoint)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded is not None
        assert loaded.pipeline_id == checkpoint.pipeline_id
        assert loaded.stream_id == checkpoint.stream_id
        assert loaded.offset == checkpoint.offset

    def test_load_returns_none_for_missing(self, store):
        result = store.load("nope", "nope")
        assert result is None

    def test_delete_removes_checkpoint(self, store, checkpoint):
        store.save(checkpoint)
        store.delete(checkpoint.pipeline_id, checkpoint.stream_id)
        assert store.load(checkpoint.pipeline_id, checkpoint.stream_id) is None

    def test_delete_missing_does_not_raise(self, store):
        store.delete("ghost", "ghost")  # should not raise

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(
            pipeline_id=checkpoint.pipeline_id,
            stream_id=checkpoint.stream_id,
            offset=999,
        )
        store.save(updated)
        loaded = store.load(checkpoint.pipeline_id, checkpoint.stream_id)
        assert loaded.offset == 999

    def test_list_checkpoints(self, store):
        cp1 = Checkpoint(pipeline_id="pipeA", stream_id="s1", offset=1)
        cp2 = Checkpoint(pipeline_id="pipeA", stream_id="s2", offset=2)
        cp3 = Checkpoint(pipeline_id="pipeB", stream_id="s1", offset=3)
        for cp in (cp1, cp2, cp3):
            store.save(cp)
        results = store.list_checkpoints("pipeA")
        assert len(results) == 2
        stream_ids = {cp.stream_id for cp in results}
        assert stream_ids == {"s1", "s2"}

    def test_list_checkpoints_returns_empty_for_unknown_pipeline(self, store):
        cp1 = Checkpoint(pipeline_id="pipeA", stream_id="s1", offset=1)
        store.save(cp1)
        results = store.list_checkpoints("unknown_pipe")
        assert results == []
