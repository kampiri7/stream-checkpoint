import json
import pytest
from stream_checkpoint.base import Checkpoint
from stream_checkpoint.backends.cloudflare_kv_store import CloudflareKVCheckpointStore


class FakeCloudflareKVClient:
    def __init__(self):
        self._store = {}

    def put(self, namespace_id, key, value):
        self._store[(namespace_id, key)] = value

    def get(self, namespace_id, key):
        return self._store.get((namespace_id, key))

    def delete(self, namespace_id, key):
        self._store.pop((namespace_id, key), None)

    def list_keys(self, namespace_id, prefix=""):
        return [
            k for (ns, k) in self._store
            if ns == namespace_id and k.startswith(prefix)
        ]


@pytest.fixture
def client():
    return FakeCloudflareKVClient()


@pytest.fixture
def store(client):
    return CloudflareKVCheckpointStore(client, namespace_id="ns1", prefix="ckpt")


@pytest.fixture
def checkpoint():
    return Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=42, metadata={"k": "v"})


class TestCloudflareKVCheckpointStore:
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

    def test_delete_nonexistent_does_not_raise(self, store):
        store.delete("ghost", "ghost")

    def test_list_checkpoints(self, store):
        c1 = Checkpoint(pipeline_id="pipe2", stream_id="s1", offset=1)
        c2 = Checkpoint(pipeline_id="pipe2", stream_id="s2", offset=2)
        c3 = Checkpoint(pipeline_id="other", stream_id="s3", offset=3)
        store.save(c1)
        store.save(c2)
        store.save(c3)
        results = store.list_checkpoints("pipe2")
        assert len(results) == 2
        stream_ids = {r.stream_id for r in results}
        assert stream_ids == {"s1", "s2"}

    def test_save_overwrites_existing(self, store, checkpoint):
        store.save(checkpoint)
        updated = Checkpoint(pipeline_id="pipe1", stream_id="stream1", offset=99)
        store.save(updated)
        loaded = store.load("pipe1", "stream1")
        assert loaded.offset == 99
